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
            var todayStr = DateTime.Now.ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
            var datadir = Directory.GetFiles(Settings.SourceDirPath).Select(p => Path.GetFileName(p));
            var archived = Directory.GetFiles(Settings.OutputDirPath).Select(p => Path.GetFileName(p));

            // If copy all remote files found
            if (!string.IsNullOrEmpty(Settings.CopyAllDirPath))
            {
                var toCopy = Directory.GetFiles(Settings.CopyAllDirPath)
                    .Where(f => f.EndsWith($".{ext}"))
                    .Where(f => !archived.Contains(Path.GetFileName(f)))
                    .Where(f => !datadir.Contains(Path.GetFileName(f)))
                    .ToArray();

                if (Settings.IgnoreCurrent)
                {
                    toCopy = toCopy.Where(f => !f.Contains(todayStr)).ToArray();
                }

                foreach (var file in toCopy)
                {
                    try 
                    {
                        var outfile = Path.Combine(Settings.SourceDirPath, Path.GetFileName(file));
                        File.Copy(file, Path.Combine(file, outfile), true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error copying remote file: {ex.Message}");
                    }
                }
            }

            // If copy only latest file
            if (!string.IsNullOrEmpty(Settings.CopyLatestDirPath))
            {
                var toCopy = "";

                if (Settings.IgnoreCurrent)
                {
                    toCopy = Directory.GetFiles(Settings.CopyLatestDirPath)
                        .Where(f => f.EndsWith($".{ext}"))
                        .Where(f => !archived.Contains(Path.GetFileName(f)))
                        .Where(f => !datadir.Contains(Path.GetFileName(f)))
                        .Where(f => !f.Contains(todayStr))
                        .OrderByDescending(f => f)
                        .FirstOrDefault();
                }
                else
                {
                    toCopy = Directory.GetFiles(Settings.CopyLatestDirPath)
                        .Where(f => f.EndsWith($".{ext}"))
                        .Where(f => !archived.Contains(Path.GetFileName(f)))
                        .Where(f => !datadir.Contains(Path.GetFileName(f)))
                        .OrderByDescending(f => f)
                        .FirstOrDefault();
                }

                if (toCopy != null)
                {
                    try
                    {
                        var outfile = Path.Combine(Settings.SourceDirPath, Path.GetFileName(toCopy));
                        File.Copy(toCopy, Path.Combine(toCopy, outfile), true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error copying remote file: {ex.Message}");
                    }
                }
            }

            // If copy a specific file
            if (!string.IsNullOrWhiteSpace(Settings.SpecificFile))
            {
                var path = Path.Combine(Settings.SourceDirPath, Settings.SpecificFile);
                if (!File.Exists(path))
                {
                    Console.WriteLine($"The specified file '{Settings.SpecificFile}' could not be found.");
                    return;
                }
                Files = new string[] { path };
                return;
            }

            // Redundant check but to be safe, delete any files already archived but mistakenly copied over again
            var pulled = Directory.GetFiles(Settings.SourceDirPath).Select(p => Path.GetFileName(p));
            foreach (var file in archived.Intersect(pulled))
            {
                try
                {
                    Directory.Delete(Path.Combine(Settings.SourceDirPath, file));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error deleting file: {ex.Message}");
                }
            }

            Files = Directory.GetFiles(Settings.SourceDirPath)
                .Where(f => f.EndsWith($".{ext}"))
                .ToArray();

            if (Settings.IgnoreCurrent)
            {
                Files = Files.Where(f => !f.Contains(todayStr)).ToArray();
            }
        }
    }
}
