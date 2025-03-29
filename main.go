package main

import (
	"context"
	"embed"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/text/language"
)

//go:embed locales/*
var localeFS embed.FS

const (
	StatementTypeNoExplainCheck = ""
	StatementTypeSelect         = "SELECT"
	StatementTypeInsert         = "INSERT"
	StatementTypeUpdate         = "UPDATE"
	StatementTypeDelete         = "DELETE"
)

var (
	DSN string

	ReadOnly         bool
	WithExplainCheck bool

	DB *sqlx.DB

	Transport string
	IPaddress string
	Port      int

	Lang string
)

type ExplainResult struct {
	Id           *string `db:"id"`
	SelectType   *string `db:"select_type"`
	Table        *string `db:"table"`
	Partitions   *string `db:"partitions"`
	Type         *string `db:"type"`
	PossibleKeys *string `db:"possible_keys"`
	Key          *string `db:"key"`
	KeyLen       *string `db:"key_len"`
	Ref          *string `db:"ref"`
	Rows         *string `db:"rows"`
	Filtered     *string `db:"filtered"`
	Extra        *string `db:"Extra"`
}

type ShowCreateTableResult struct {
	Table       string `db:"Table"`
	CreateTable string `db:"Create Table"`
}

func main() {

	// 初始化i18n
	bundle := i18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("toml", toml.Unmarshal)

	flag.StringVar(&DSN, "dsn", "", "POSTGRES DSN")
	flag.BoolVar(&ReadOnly, "read-only", false, "Enable read-only mode")
	flag.BoolVar(&WithExplainCheck, "with-explain-check", false, "Check query plan with `EXPLAIN` before executing")

	flag.StringVar(&Transport, "t", "stdio", "Transport type (stdio or sse)")
	flag.IntVar(&Port, "port", 8080, "sse server port")
	flag.StringVar(&IPaddress, "ip", "localhost", "server ip address")

	flag.StringVar(&Lang, "lang", language.English.String(), "Language code (en/zh-CN/...)")

	flag.Parse()

	langTag, err := language.Parse(Lang)
	if err != nil {
		langTag = language.English
	}

	langFile := fmt.Sprintf("locales/%s/active.%s.toml", langTag.String(), langTag.String())
	if data, err := localeFS.ReadFile(langFile); err == nil {
		bundle.ParseMessageFileBytes(data, langFile)
	} else {
		if enData, err := localeFS.ReadFile("locales/en/active.en.toml"); err == nil {
			bundle.ParseMessageFileBytes(enData, "locales/en/active.en.toml")
		}
	}

	localizer := i18n.NewLocalizer(bundle, langTag.String())

	T := func(key string) string {
		return localizer.MustLocalize(&i18n.LocalizeConfig{MessageID: key})
	}

	s := server.NewMCPServer(
		"go-mcp-postgres",
		"0.2.1",
		server.WithResourceCapabilities(true, true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
	)

	// Schema Tools
	listDatabaseTool := mcp.NewTool(
		"list_database",
		mcp.WithDescription(T("gomcp.list_database")),
	)

	listTableTool := mcp.NewTool(
		"list_table",
		mcp.WithDescription(T("gomcp.list_table")),
	)

	createTableTool := mcp.NewTool(
		"create_table",
		mcp.WithDescription(T("gomcp.create_table")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.create_table_query_description")),
		),
	)

	alterTableTool := mcp.NewTool(
		"alter_table",
		mcp.WithDescription(T("gomcp.alter_table")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.alter_table_query")),
		),
	)

	descTableTool := mcp.NewTool(
		"desc_table",
		mcp.WithDescription(T("gomcp.desc_table")),
		mcp.WithString("name",
			mcp.Required(),
			mcp.Description(T("gomcp.desc_table_name")),
		),
	)

	// Data Tools
	readQueryTool := mcp.NewTool(
		"read_query",
		mcp.WithDescription(T("gomcp.read_query")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.query_execute_description")),
		),
	)

	countQueryTool := mcp.NewTool(
		"count_query",
		mcp.WithDescription(T("gomcp.count_query")),
		mcp.WithString("name",
			mcp.Required(),
			mcp.Description(T("gomcp.count_query_name")),
		),
	)

	writeQueryTool := mcp.NewTool(
		"write_query",
		mcp.WithDescription(T("gomcp.write_query")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.query_execute_description")),
		),
	)

	updateQueryTool := mcp.NewTool(
		"update_query",
		mcp.WithDescription(T("gomcp.update_query")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.query_execute_description")),
		),
	)

	deleteQueryTool := mcp.NewTool(
		"delete_query",
		mcp.WithDescription(T("gomcp.delete_query")),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description(T("gomcp.query_execute_description")),
		),
	)

	s.AddTool(listDatabaseTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery("SELECT datname FROM pg_database WHERE datistemplate = false;", StatementTypeNoExplainCheck)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(listTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery("SELECT table_schema,table_name FROM information_schema.tables ORDER BY table_schema,table_name;", StatementTypeNoExplainCheck)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})

	if !ReadOnly {
		s.AddTool(createTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := HandleExec(request.Params.Arguments["query"].(string), StatementTypeNoExplainCheck)
			if err != nil {
				return nil, nil
			}

			return mcp.NewToolResultText(result), nil
		})
	}

	if !ReadOnly {
		s.AddTool(alterTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := HandleExec(request.Params.Arguments["query"].(string), StatementTypeNoExplainCheck)
			if err != nil {
				return nil, nil
			}

			return mcp.NewToolResultText(result), nil
		})
	}
	s.AddTool(listTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery("SELECT table_schema,table_name FROM information_schema.tables ORDER BY table_schema,table_name;", StatementTypeNoExplainCheck)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(descTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		descsql :=
			`SELECT
    'CREATE TABLE ' || t.table_name || ' (' ||
    string_agg(
        c.column_name || ' ' || c.data_type ||
        CASE 
            WHEN c.character_maximum_length IS NOT NULL THEN '(' || c.character_maximum_length || ')'
            ELSE ''
        END ||
        CASE 
            WHEN c.is_nullable = 'NO' THEN ' NOT NULL'
            ELSE ''
        END, ', '
    ) ||
    ', PRIMARY KEY (' || (
        SELECT string_agg(kcu.column_name, ', ')
        FROM information_schema.key_column_usage kcu
        WHERE kcu.table_name = t.table_name AND kcu.constraint_name LIKE '%_pkey'
    ) || ')' ||
    ');' AS create_table_sql
FROM
    information_schema.tables t
JOIN
    information_schema.columns c ON t.table_name = c.table_name
WHERE
    t.table_name = '` + request.Params.Arguments["name"].(string) + `'
GROUP BY
    t.table_name;`
		result, err := HandleQuery(descsql, StatementTypeNoExplainCheck)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(readQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery(request.Params.Arguments["query"].(string), StatementTypeSelect)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})
	s.AddTool(countQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery("SELECT count(1) from "+request.Params.Arguments["name"].(string)+";", StatementTypeNoExplainCheck)
		if err != nil {
			return nil, nil
		}

		return mcp.NewToolResultText(result), nil
	})

	if !ReadOnly {
		s.AddTool(writeQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := HandleExec(request.Params.Arguments["query"].(string), StatementTypeInsert)
			if err != nil {
				return nil, nil
			}

			return mcp.NewToolResultText(result), nil
		})
	}

	if !ReadOnly {
		s.AddTool(updateQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := HandleExec(request.Params.Arguments["query"].(string), StatementTypeUpdate)
			if err != nil {
				return nil, nil
			}

			return mcp.NewToolResultText(result), nil
		})
	}

	if !ReadOnly {
		s.AddTool(deleteQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := HandleExec(request.Params.Arguments["query"].(string), StatementTypeDelete)
			if err != nil {
				return nil, nil
			}

			return mcp.NewToolResultText(result), nil
		})
	}

	// Only check for "sse" since stdio is the default
	if Transport == "sse" {
		sseServer := server.NewSSEServer(s, server.WithBaseURL(fmt.Sprintf("http://%s:%d", IPaddress, Port)))
		//log.Printf("SSE server listening on : %d", Port)
		if err := sseServer.Start(fmt.Sprintf("%s:%d", IPaddress, Port)); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	} else {
		if err := server.ServeStdio(s); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}

}

func GetDB() (*sqlx.DB, error) {
	if DB != nil {
		return DB, nil
	}

	db, err := sqlx.Connect("pgx", DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to establish database connection: %v", err)
	}

	DB = db

	return DB, nil
}

func HandleQuery(query, expect string) (string, error) {
	result, headers, err := DoQuery(query, expect)
	if err != nil {
		return "", err
	}

	s, err := MapToCSV(result, headers)
	if err != nil {
		return "", err
	}

	return s, nil
}

func DoQuery(query, expect string) ([]map[string]interface{}, []string, error) {
	db, err := GetDB()
	if err != nil {
		return nil, nil, err
	}

	if len(expect) > 0 {
		if err := HandleExplain(query, expect); err != nil {
			return nil, nil, err
		}
	}

	rows, err := db.Queryx(query)
	if err != nil {
		return nil, nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	result := []map[string]interface{}{}
	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			return nil, nil, err
		}

		resultRow := map[string]interface{}{}
		for i, col := range cols {
			switch v := row[i].(type) {
			case []byte:
				resultRow[col] = string(v)
			default:
				resultRow[col] = v
			}
		}
		result = append(result, resultRow)
	}

	return result, cols, nil
}

func HandleExec(query, expect string) (string, error) {
	db, err := GetDB()
	if err != nil {
		return "", err
	}

	if len(expect) > 0 {
		if err := HandleExplain(query, expect); err != nil {
			return "", err
		}
	}

	result, err := db.Exec(query)
	if err != nil {
		return "", err
	}

	ra, err := result.RowsAffected()
	if err != nil {
		return "", err
	}

	switch expect {
	case StatementTypeInsert:
		li, err := result.LastInsertId()
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%d rows affected, last insert id: %d", ra, li), nil
	default:
		return fmt.Sprintf("%d rows affected", ra), nil
	}
}

func HandleExplain(query, expect string) error {
	if !WithExplainCheck {
		return nil
	}

	db, err := GetDB()
	if err != nil {
		return err
	}

	rows, err := db.Queryx(fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		return err
	}

	result := []ExplainResult{}
	for rows.Next() {
		var row ExplainResult
		if err := rows.StructScan(&row); err != nil {
			return err
		}
		result = append(result, row)
	}

	if len(result) != 1 {
		return fmt.Errorf("unable to check query plan, denied")
	}

	match := false
	switch expect {
	case StatementTypeInsert:
		fallthrough
	case StatementTypeUpdate:
		fallthrough
	case StatementTypeDelete:
		if *result[0].SelectType == expect {
			match = true
		}
	default:
		// for SELECT type query, the select_type will be multiple values
		// here we check if it's not INSERT, UPDATE or DELETE
		match = true
		for _, typ := range []string{StatementTypeInsert, StatementTypeUpdate, StatementTypeDelete} {
			if *result[0].SelectType == typ {
				match = false
				break
			}
		}
	}

	if !match {
		return fmt.Errorf("query plan does not match expected pattern, denied")
	}

	return nil
}

/*
func HandleDescTable(name string) (string, error) {
	db, err := GetDB()
	if err != nil {
		return "", err
	}

	rows, err := db.Queryx(descsql)
	if err != nil {
		return "", err
	}

	result := []ShowCreateTableResult{}
	for rows.Next() {
		var row ShowCreateTableResult
		if err := rows.StructScan(&row); err != nil {
			return "", err
		}
		result = append(result, row)
	}

	if len(result) == 0 {
		return "", fmt.Errorf("table %s does not exist", name)
	}

	return result[0].CreateTable, nil
}*/

func MapToCSV(m []map[string]interface{}, headers []string) (string, error) {
	var csvBuf strings.Builder
	writer := csv.NewWriter(&csvBuf)

	if err := writer.Write(headers); err != nil {
		return "", fmt.Errorf("failed to write headers: %v", err)
	}

	for _, item := range m {
		row := make([]string, len(headers))
		for i, header := range headers {
			value, exists := item[header]
			if !exists {
				return "", fmt.Errorf("key '%s' not found in map", header)
			}
			row[i] = fmt.Sprintf("%v", value)
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write row: %v", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("error flushing CSV writer: %v", err)
	}

	return csvBuf.String(), nil
}
