# SQL-to-Graphite

A tool to easily send the results of SQL queries to Graphite!

## Installation

```
pip install sql-to-graphite
```

## Running

```
export S2G_DSN="mysq://username:password@host/db"
cat queries.sql | sql-to-graphite --graphite-host graphite.example.com --graphite-prefix db.metrics --dsn "mssql://<user>:<password>@ServerDSN" --timestamped-metric
```

The queries piped in should be a single query per line returning 2 columns. If there are more columns they will be ignored. The first column returned should be the metric name (minus the --graphite-prefix option) and the value.

```
SELECT 'example.test', 3, 1495670400;
```
