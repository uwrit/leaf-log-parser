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
            "copy-all",
            Required = false,
            HelpText = "Copy all (remote or local) log files found in a directory to source directory")]
        public string CopyAllDirPath { get; set; }

        [Option(
            "copy-latest",
            Required = false,
            HelpText = "Copy latest (remote or local) log file found in a directory to source directory")]
        public string CopyLatestDirPath { get; set; }

        [Option(
            'o',
            "output",
            Required = false,
            Default = "archive",
            HelpText = "The output directory to move Leaf logs to after processing."
        )]
        public string OutputDirPath { get; set; }

        [Option(
            'd',
            "database",
            Required = true,
            HelpText = "The database connection string to connect to SQL Server. " +
                       "Should be of the form 'Server=<address>;Database=<db_name>;User Id=<user_name>;Password=<pass>;Integrated Security=<optional, if using Windows auth then 'SSPI', omit pass and user>.'")]
        public string DbConnection { get; set; }

        [Option(
            't',
            "table",
            Required = false,
            Default = "dbo.UsageLog",
            HelpText = "The database table in which to copy the log file information into. Should be of the form <schema>.<table_name>."
        )]
        public string DbTable { get; set; }

        [Option(
            'b',
            "batch-size",
            Required = false,
            Default = 1000,
            HelpText = "Number of log records to build insert into the database at a time."
        )]
        public int BatchSize { get; set; }

        [Option(
            'i',
            "ignored-message-types",
            Required = false,
            Separator = ',',
            Default = new string[] { "Refreshed TokenBlacklistCache", "Refreshed InvalidatedTokenCache", "Refreshed ServerState" },
            HelpText = "Log entry MessageTemplate types to ignore, delimited by ','."
        )]
        public IEnumerable<string> IgnoredTypes { get; set; }

        [Option(
            'c',
            "ignore-current",
            Required = false,
            Default = true,
            HelpText = "Boolean indicating whether files named with the same date as the current date should be ignored."
        )]
        public bool IgnoreCurrent { get; set; }

        [Option(
            'n',
            "no-archive",
            Required = false,
            Default = false,
            HelpText = "Boolean indicating whether completed log files should not be moved to the output directory after processing."
        )]
        public bool PreventArchive { get; set; }

        [Option(
            'f',
            "specific-file",
            Required = false,
            HelpText = "An optional specific file to parse in the source directory."
        )]
        public string SpecificFile { get; set; }
    }
}
