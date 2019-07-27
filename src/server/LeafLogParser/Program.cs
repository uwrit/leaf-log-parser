using System;
using System.IO;
using System.Data.SqlClient;
using System.Threading.Tasks;
using CommandLine;
using Model;
using System.Collections.Generic;

namespace LeafLogParser
{
    class Program
    {
        static AppSettings settings = new AppSettings();

        static async Task Main(string[] args)
        {
            /* VS debug
            settings.SourceDirPath = "C:/NicWork/apps/logs/leaf";
            settings.OutputDirPath = settings.SourceDirPath + "/archive";
            settings.DbConnection = "Server=localhost;Database=LeafLog;Integrated Security=SSPI";
            settings.DbTable = "dbo.UsageLog";
            settings.BatchSize = 1000;
            settings.IgnoreTypes = new string[] { "Refreshed TokenBlacklistCache" };
            settings.IgnoreToday = true;
            settings.MoveCompleted = true;
            */

            Console.WriteLine($"Starting up Leaf Log Reader...");

            LoadSettings(args);
            CheckDirectories();
            await CheckDatabase();

            var manager = new LogEntryTransferManager(settings);
            var reader = new LogReader(settings, manager);

            if (!reader.FilesFound)
            {
                Console.WriteLine($"No files found to parse. Exiting...");
                return;
            }

            while (reader.Read())
            {
                await reader.Process();
            }

            Console.WriteLine($"Successfully copied {manager.CopyCount} log entries from {reader.FileCount} file(s).");
            Console.WriteLine($"Exiting...");
        }

        static void LoadSettings(string[] args)
        {
            Parser.Default.ParseArguments<AppSettings>(args)
                .WithParsed(s => settings = s);

            // If no separator in the output path, assume it's within the source directory
            if (!settings.OutputDirPath.Contains(Path.DirectorySeparatorChar))
            {
                var outname = settings.OutputDirPath;
                settings.OutputDirPath = $"{settings.SourceDirPath}{Path.DirectorySeparatorChar}{outname}";
            }
        }

        static void CheckDirectories()
        {
            var message = "";

            if (!Directory.Exists(settings.SourceDirPath))
            {
                message = $"The source directory '{settings.SourceDirPath}' could not be found.";
                Console.WriteLine(message);
                throw new DirectoryNotFoundException(message);
            }

            if (!Directory.Exists(settings.OutputDirPath))
            {
                message = $"The output directory '{settings.OutputDirPath}' could not be found. Attempting to create...";
                Console.WriteLine(message);

                try
                {
                    Directory.CreateDirectory(settings.OutputDirPath);
                    message = $"Output directory '{settings.OutputDirPath}' created.";
                    Console.WriteLine(message);
                }
                catch (Exception ex)
                {
                    message = $"The output directory '{settings.OutputDirPath}' could not be created: {ex.Message}";
                    throw new Exception(message, ex.InnerException);
                }
            }

            message = $"Directories '{settings.SourceDirPath}' and '{settings.OutputDirPath}' successfully validated.";
            Console.WriteLine(message);
        }

        static async Task CheckDatabase()
        {
            var message = "";
            var sql = $"SELECT TOP 1 * FROM {settings.DbTable}";

            try
            {
                using (SqlConnection conn = new SqlConnection(settings.DbConnection))
                {

                    conn.Open();
                    var cmd = new SqlCommand(sql, conn);
                    await cmd.ExecuteNonQueryAsync();

                    message = $"SQL Server '{conn.Database}', table '{settings.DbTable}' successfully connected.";
                    Console.WriteLine(message);
                }
            }
            catch (Exception ex)
            {
                message = $"The SQL Server returned an error: {ex.Message}";
                throw new ArgumentException(message, "t", ex.InnerException);
            }
        }
    }
}
