# go-mcp-postgres

## Overview

Copy code from https://github.com/Zhwt/go-mcp-mysql/ and with ai help, I change db from mysql to postgres.
Zero burden, ready-to-use Model Context Protocol (MCP) server for interacting with Postgres and automation. No Node.js or Python environment needed. This server provides tools to do CRUD operations on MySQL databases and tables, and a read-only mode to prevent surprise write operations. You can also make the MCP server check the query plan by using a `EXPLAIN` statement before executing the query by adding a `--with-explain-check` flag.

Please note that this is a work in progress and may not yet be ready for production use.

## Installation

1. Get the latest [release](https://github.com/guoling2008/go-mcp-postgres/releases) and put it in your `$PATH` or somewhere you can easily access.

2. Or if you have Go installed, you can build it from source:

```sh
go install -v github.com/guoling2008/go-mcp-postgres@latest
```

## Usage

### Method A: Using Command Line Arguments for stdio mode

```json
{
  "mcpServers": {
    "postgres": {
      "command": "go-mcp-postgres",
      "args": [
        "--dsn",
        "postgresql://user:pass@host:port/db"
      ]
    }
  }
}
```



Note: For those who put the binary outside of your `$PATH`, you need to replace `go-mcp-postgres` with the full path to the binary: e.g.: if you put the binary in the **Downloads** folder, you may use the following path:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "C:\\Users\\<username>\\Downloads\\go-mcp-postgres.exe",
      "args": [
        ...
      ]
    }
  }
}
```

### Method B: Using Command Line Arguments for sse mode

./go-mcp-postgres --t sse --ip x.x.x.x --port nnnn --dsn postgresql://user:pass@host:port/db

### Optional Flags

- Add a `--read-only` flag to enable read-only mode. In this mode, only tools beginning with `list`, `read_` and `desc_` are available. Make sure to refresh/restart the MCP server after adding this flag.
- By default, CRUD queries will be first executed with a `EXPLAIN ?` statement to check whether the generated query plan matches the expected pattern. Add a `--with-explain-check` flag to disable this behavior.

## Tools

### Schema Tools

1. `list_database`

    - List all databases in the Postgres server.
    - Parameters: None
    - Returns: A list of matching database names.

2. `list_table`

    - List all tables in the Postgres server.
    - Parameters:
        - `name`: If provided, list tables with the specified name, Otherwise, list all tables.
    - Returns: A list of matching table names.

3. `create_table`

    - Create a new table in the Postgres server.
    - Parameters:
        - `query`: The SQL query to create the table.
    - Returns: x rows affected.

4. `alter_table`

    - Alter an existing table in the Postgres server. The LLM is informed not to drop an existing table or column.
    - Parameters:
        - `query`: The SQL query to alter the table.
    - Returns: x rows affected.

5. `desc_table`

    - Describe the structure of a table.
    - Parameters:
        - `name`: The name of the table to describe.
    - Returns: The structure of the table.
  
### Data Tools

1. `read_query`

    - Execute a read-only SQL query.
    - Parameters:
        - `query`: The SQL query to execute.
    - Returns: The result of the query.

2. `write_query`

    - Execute a write SQL query.
    - Parameters:
        - `query`: The SQL query to execute.
    - Returns: x rows affected, last insert id: <last_insert_id>.

3. `update_query`

    - Execute an update SQL query.
    - Parameters:
        - `query`: The SQL query to execute.
    - Returns: x rows affected.

4. `delete_query`

    - Execute a delete SQL query.
    - Parameters:
        - `query`: The SQL query to execute.
    - Returns: x rows affected.
    
5. `count_query`

    - Query the number of rows in a certain table..
    - Parameters:
        - `name`: The name of the table to count.
    - Returns: The row number of the table.
    
Big thanks to https://github.com/Zhwt/go-mcp-mysql/ again.

## License

MIT
