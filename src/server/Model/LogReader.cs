using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Model
{
    public class LogReader
    {
        const char LeftBrace = '{';
        const char RightBrace = '}';
        readonly HashSet<string> IgnoredTypes;

        AppSettings Settings { get; set; }
        LogEntryTransferManager Manager { get; set; }

        string[] Files = new string[] { };
        int CurrFileIndex = -1;

        public int FileCount => Files.Length;

        public bool FilesFound => Files.Length > 0;

        public LogReader(AppSettings settings, LogEntryTransferManager manager)
        {
            this.Settings = settings;
            this.Manager = manager;
            this.IgnoredTypes = new HashSet<string>(settings.IgnoredTypes);
            GetFiles();
        }

        public bool Read()
        {
            CurrFileIndex++;
            if (CurrFileIndex <= Files.Length - 1)
            {
                return true;
            }
            return false;
        }

        public async Task Process()
        {
            var curr = Files[CurrFileIndex];
            var braceCount = 0;
            var chars = new List<char>();
            char ch;
            
            using (StreamReader sr = new StreamReader(curr))
            {
                while (!sr.EndOfStream)
                {
                    ch = (char)sr.Read();
                    chars.Add(ch);

                    if (ch == LeftBrace)
                    {
                        braceCount++;
                    }
                    else if (ch == RightBrace)
                    {
                        braceCount--;

                        if (braceCount == 0)
                        {
                            var json = new string(chars.ToArray());
                            try
                            {
                                var record = JsonConvert.DeserializeObject<LogRecord>(json);
                                var entry = record.ToLogEntry();

                                if (IgnoredTypes.Contains(entry.MessageTemplate))
                                {
                                    chars.Clear();
                                    continue;
                                }

                                Manager.Add(entry);

                                if (Manager.RowCount == Settings.BatchSize)
                                {
                                    await Manager.ToSql();
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Unparsable log entry found. JSON: {json}. Error: {ex.Message}");
                            }

                            chars.Clear();
                        }
                    }
                }

                if (Manager.RowCount > 0)
                {
                    await Manager.ToSql();
                }
            }

            if (!Settings.PreventArchive)
            {
                var outpath = $"{Settings.OutputDirPath}{Path.DirectorySeparatorChar}{Path.GetFileName(curr)}";
                File.Move(curr, outpath);
            }

            Console.WriteLine($"Completed {curr}...");
        }

        void GetFiles()
        {
            var ext = "log";

            // If copy all remote files found
            if (!string.IsNullOrEmpty(Settings.CopyAllDirPath))
            {
                var toCopy = Directory.GetFiles(Settings.CopyAllDirPath)
                    .Where(f => f.EndsWith($".{ext}"))
                    .ToArray();

                foreach (var file in toCopy)
                {
                    var outfile = Path.Combine(Settings.SourceDirPath, Path.GetFileName(file));
                    File.Copy(file, Path.Combine(file, outfile), true);
                }
            }

            // If copy only latest file
            if (!string.IsNullOrEmpty(Settings.CopyLatestDirPath))
            {
                var toCopy = Directory.GetFiles(Settings.CopyLatestDirPath)
                    .Where(f => f.EndsWith($".{ext}"))
                    .OrderByDescending(f => f)
                    .FirstOrDefault();

                if (toCopy != null)
                {
                    var outfile = Path.Combine(Settings.SourceDirPath, Path.GetFileName(toCopy));
                    File.Copy(toCopy, Path.Combine(toCopy, outfile), true);
                }
            }

            // If copy a specific file
            if (!string.IsNullOrWhiteSpace(Settings.SpecificFile))
            {
                var path = $"{Settings.SourceDirPath}{Path.DirectorySeparatorChar}{Settings.SpecificFile}";
                if (!File.Exists(path))
                {
                    Console.WriteLine($"The specified file '{Settings.SpecificFile}' could not be found.");
                    return;
                }
                Files = new string[] { path };
                return;
            }

            Files = Directory.GetFiles(Settings.SourceDirPath)
                .Where(f => f.EndsWith($".{ext}"))
                .ToArray();

            if (Settings.IgnoreCurrent)
            {
                var todayStr = DateTime.Now.ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                Files = Files.Where(f => !f.Contains(todayStr)).ToArray();
            }
        }
    }
}
