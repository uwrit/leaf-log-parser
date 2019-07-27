# LeafLogParser &middot; [![GitHub license](https://img.shields.io/badge/license-BSD3-blue.svg)](https://github.com/facebook/react/blob/master/LICENSE) ![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)
LeafLogParser is a quick, simple app for copying [Leaf application](https://github.com/uwrit/leaf) log file data to a SQL Server and querying via JSON-based views.

The Leaf API logs a vast amount of useful data on to log files as users use the application. These include data on what users are querying, how long queries take, SQL compilation, logins, errors, and security. Each log *entry* within a log file is structured as a [JSON](https://www.json.org/) Object of the form:

```
// Example
{
    "Timestamp": "2019-07-22T15:10:10.5540648-07:00",
    "Level": "Information",
    "MessageTemplate": "FullCount cohort retrieved. Cohort:{@Cohort}",
    "Properties": {
        ...
    }
}
```

Logging to files (as opposed directly to the database) is an industry standard and ensures that the Leaf API responds to user requests quickly and efficiently. Yet for analytical and auditing reasons is still often important to ensure that log information is copied to a technology better suited to analytics, such as a relational database.

One more complication is the fact that while the `Timestamp`, `Level`, and `MessageTemplate` properties shown in the example above are consistent and predictable (i.e., they appear in every log entry, no matter the context), the *contents* of the `Properties` field vary greatly depending on the situation and what methods and variables are involved.

**The LeafLogParser is a straightforward solution to this problem. It:** 
1) Streams through notes, parsing and adding a select number of other useful fields while preserving the `Properties` data.
2) Efficiently copies the data to a SQL table.
3) Provides out-of-the-box SQL views representing transforms of the data to answer different questions.

We've found this to work well at the University of Washington, as it allows us to preserve the source log data while being able to flexibly and quickly create new SQL views to answer different questions.

Thus this:

```sql
-- Raw data
SELECT TOP 10 *
FROM dbo.UsageLog
WHERE MessageTemplate = 'FullCount cohort retrieved. Cohort:{@Cohort}'
```

| Timestamp  | Level       | MessageTemplate                              | Properties                                     |
| ---------- | ----------- | -------------------------------------------- | ---------------------------------------------- |
| 2019-07-22 | Information | FullCount cohort retrieved. Cohort:{@Cohort} | { "Cohort": { "Count": 46, "SqlStatements" ... |
| ...        |             |                                              |                                                |

Can be quickly and flexibly analyzed with a simple view that parses `Properties`:

```sql
-- JSON-transformed view of the same data to find patient count queries
SELECT TOP 10 *
FROM dbo.v_CountQuery
```

| Timestamp  | User       | Count | SqlStatements                                        | ExecutionTime |
| ---------- | ---------- | ----- | ---------------------------------------------------- | ------------- |
| 2019-07-22 | ndobb@leaf | 46    | WITH wrapper (personId) AS ( SELECT P0.SUBJECT_ID... | 2.6           |
| ...        |            |       |                                                      |               |

Note that the **LeafLogParser** is merely one way to solve this problem, and we greatly appreciate thoughts and ideas of how to improve it. Please feel free to open an issue, make a pull request, or fork it to meet your needs :smiley:

## Requirements

1) [.NET Core 2.2+ runtime](https://dotnet.microsoft.com/download) installed on the server the log files are stored. As this is typically the server hosting your Leaf API, this is likely already installed.
2) A database server with [MS SQL Server 16+](https://www.microsoft.com/en-us/sql-server/default.aspx) installed. A newer version of SQL Server is necessary to take advantage of [JSON-parsing functionality](https://docs.microsoft.com/en-us/sql/relational-databases/json/json-data-sql-server?view=sql-server-2017) introduced in SQL Server 2016.

## Installation

### DB Server

Logs are transformed and written to a database table with column names matching the [properties found in log entries](src/server/Model/UsageLog.cs#L31).

Create the database (adding arguments and environment-specific details as needed).
```sql
CREATE DATABASE <LeafLogDB>
```

Create the table and views under [src/db/build.sql](src/db/build.sql).
```sql
USE <LeafLogDB>
GO

CREATE TABLE [dbo].[UsageLog](
	[Id] [uniqueidentifier] NOT NULL,
	[Timestamp] [datetime] NULL,
	[Level] [nvarchar](50) NULL,
	[MessageTemplate] [nvarchar](200) NULL,
	[Properties] [nvarchar](max) NULL,
	[Renderings] [nvarchar](max) NULL,
	[ActionId] [nvarchar](200) NULL,
	[ActionName] [nvarchar](200) NULL,
	[ConnectionId] [nvarchar](200) NULL,
	[RequestId] [nvarchar](200) NULL,
	[RequestPath] [nvarchar](200) NULL,
	[SessionId] [nvarchar](200) NULL,
	[SourceContext] [nvarchar](200) NULL,
	[User] [nvarchar](200) NULL,
 CONSTRAINT [PK_UsageLog] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)
GO

ALTER TABLE [dbo].[UsageLog] ADD  CONSTRAINT [DF_UsageLog_Id]  DEFAULT (newsequentialid()) FOR [Id]
GO

-- Additional views, etc...
```

### App Server
Clone/copy the repo to the [Leaf App server](https://github.com/uwrit/leaf/tree/master/docs/deploy#architecture), or wherever your Leaf log files are stored.
```bash
$ git clone https://github.com/uwrit/leaf-log-parser.git
```

Build and publish the app.
```bash
$ cd src/server/LeafLogParser
$ dotnet publish -c Release
```
> Note that you may need to add additional arguments, particularly if building in a Linux environment. See the [dotnet publish page](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-publish?tabs=netcore21) for more information.

This will output the published files to the `src/server/LeafLogParser/bin/Release/netcoreapp2.2` directory which can be executed with:

```bash
$ cd bin/Release/netcoreapp2.2
$ dotnet LeafLogParser.dll 
    -s "<log_directory_path>"
    -d "<sql_conn_string>"
```

### Parameters [(source)](src/server/Model/AppSettings.cs)

| Parameter                     | Required | Default        | Comments                                      |
| ----------------------------- | :------: | -------------- | --------------------------------------------- |
| -s or --source                | X        |                                 | Full path of directory where Leaf log files are stored. This is the `SERILOG_DIR` variable configured in the [Leaf environment variables](https://github.com/uwrit/leaf/blob/master/docs/deploy/app/README.md#setting-environment-variables). |
| -o or --output                |          | `archive`                       | Directory where log files should be moved after processing. Can be a full path or directory name. If it is a directory name (not a path), Leaf will create it within the `-s` directory. |
| -d or --database              | X        |                                 | Connection string for the database into which parsed log data are inserted. Should be of the form `Server=<address>;Database=<db_name>;User Id=<user_name>;Password=<pass>`. |
| -t or --table                 |          | `dbo.UsageLog`                  | Name of the schema and table into which data are inserted into. |
| -b or -batchsize              |          | 1000                            | Number of log entries by which to batch inserts into the SQL destination table. |
| -i or --ignored-message-types |          | `Refreshed TokenBlacklistCache` | Comma-delimited strings of MessageTemplates which to ignore and not insert into the database. By default this excludes messages related to TokenBlacklist caching, which are frequent and typically not useful for analysis. |
| -c or --ignore-current        |          | `true`                          | Specifies any log files whose name matches that of the current date (e.g., `leaf-api-<today>.log`) should be ignored. We recommend keeping this as `true` in order to avoid reading while the Leaf API is simultaneously writing to the file. |
| -n or --no-archive            |          | `false`                         | Specifies that processed log files should not be archived.  We recommend keeping this as `false` in order to avoid reprocessing the same log files multiple times. |
| -f or --specific-file         |          |                                 | A specific file name within the `-s` directory to be processed. This is useful when you wish to process only a particular file and not all in the directory.






