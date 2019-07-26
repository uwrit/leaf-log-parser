using System;
using System.Collections.Generic;
using System.Text;
using CommandLine;

namespace Model
{
    public class AppSettings
    {
        [Option(
            's',
            "source",
            Required = true,
            HelpText = "The target directory to extract Leaf logs from."
        )]
        public string SourceDirPath { get; set; }

        [Option(
            'o',
            "output",
            Required = false,
            Default = "archive",
            HelpText = "The output directory to move Leaf logs to after processing. Defaults to <source>/archive"
        )]
        public string OutputDirPath { get; set; }

        [Option(
            'd',
            "database",
            Required = true,
            HelpText = "The database connection string to connect to SQL Server. " +
                       "Should be of the form 'Server=<address>;Database=<db_name>;User Id=<user_name>;'Password=<pass>;Integrated Security=<optional, if using Windows auth then 'SSPI', omit pass and user>")]
        public string DbConnection { get; set; }

        [Option(
            't',
            "table",
            Required = true,
            HelpText = "The database table in which to copy the log file information into. Should be of the form <schema>.<table_name>"
        )]
        public string DbTable { get; set; }

        [Option(
            'b',
            "batch-size",
            Required = false,
            Default = 1000,
            HelpText = "Number of log records to build insert into the database at a time. Defaults to 1000."
        )]
        public int BatchSize { get; set; }

        [Option(
            'i',
            "ignore--message-types",
            Required = false,
            Separator = ',',
            Default = new string[] { "Refreshed TokenBlacklistCache" },
            HelpText = "Log entry MessageTemplate types to ignore, delimited by ','. Defaults to { 'Refreshed TokenBlacklistCache' }"
        )]
        public IEnumerable<string> IgnoreTypes { get; set; }

        [Option(
            'c',
            "ignore-today",
            Required = false,
            Default = true,
            HelpText = "Boolean indicating whether files named with the same date as the current date should be ignored. Defaults to true."
        )]
        public bool IgnoreToday { get; set; }
    }
}
