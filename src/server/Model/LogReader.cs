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
        AppSettings settings { get; set; }

        string[] files = new string[] { };

        int currFileIndex = -1;

        LogEntryTransferManager manager { get; set; }

        public int FileCount => files.Length;

        public bool FilesFound => files.Length > 0;

        public LogReader(AppSettings settings, LogEntryTransferManager manager)
        {
            this.settings = settings;
            this.manager = manager;
            GetFiles();
        }

        public bool Read()
        {
            currFileIndex++;
            if (currFileIndex <= files.Length - 1)
            {
                return true;
            }
            return false;
        }

        public async Task Process()
        {
            var curr = files[currFileIndex];
            var leftBrace = '{';
            var rightBrace = '}';
            var braceCount = 0;
            var chars = new List<char>();
            var entries = new List<LogEntry>();
            var ignore = new HashSet<string>(settings.IgnoreTypes);
            char ch;
            
            using (StreamReader sr = new StreamReader(curr))
            {
                while (!sr.EndOfStream)
                {
                    ch = (char)sr.Read();
                    chars.Add(ch);

                    if (ch == leftBrace)
                    {
                        braceCount++;
                    }
                    else if (ch == rightBrace)
                    {
                        braceCount--;

                        if (braceCount == 0)
                        {
                            var json = new string(chars.ToArray());
                            try
                            {
                                var record = JsonConvert.DeserializeObject<LogRecord>(json);
                                var entry = record.ToLogEntry();

                                if (ignore.Contains(entry.MessageTemplate))
                                {
                                    chars.Clear();
                                    continue;
                                }

                                entries.Add(entry);

                                if (entries.Count == settings.BatchSize)
                                {
                                    await manager.ToSql(entries);
                                    entries.Clear();
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

                if (entries.Count > 0)
                {
                    await manager.ToSql(entries);
                    entries.Clear();
                }
            }

            if (!settings.PreventArchive)
            {
                var outpath = $"{settings.OutputDirPath}{Path.DirectorySeparatorChar}{Path.GetFileName(curr)}";
                File.Move(curr, outpath);
            }

            Console.WriteLine($"Completed {curr}...");
        }

        void GetFiles()
        {
            var ext = "log";
            files = Directory.GetFiles(settings.SourceDirPath)
                .Where(f => f.EndsWith($".{ext}"))
                .ToArray();

            if (settings.IgnoreToday)
            {
                var todayStr = DateTime.Now.ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                files = files.Where(f => !f.Contains(todayStr)).ToArray();
            }
        }
    }
}
