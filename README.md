# SQL to Graphite with Docker

Based on [sql-to-graphite](https://github.com/sashman/sql-to-graphite).

## Using with mssql

1. Edit `odbc.ini` to contain the name and the address of your Server
2. The queries directory needs to be linked by a volume
  - The format of the fields (in the `SELECT` statement) in the query must be:

    | Metric Key | Value  | Unix Timestamp
    |-------------|-------|-
    | example.test | 123 | 1495670400
  - The queries have to be in the file called `queries.sql`

  3. Use a DSN string in the following format `"mssql://<user>:<password>@ServerDSN"`
  4. Build the image with:
  ```console
  docker build -t sql-to-graphite .
  ```
  5. Run the script (in docker) with:
  ```console
  docker run -v `pwd`/queries:/queries/ -e GRAPHITE_HOST=graphite.example.com -e PREFIX=test -e S2G_DSN="mssql://<user>:<password>@ServerDSN" sql-to-graphite
  ```
