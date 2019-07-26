using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Reflection;
using System.Linq;
using System.Threading.Tasks;

namespace Model
{
    public class LogEntryTransferManager
    {
        AppSettings settings { get; set; }

        IEnumerable<PropertyInfo> props = typeof(LogEntry).GetProperties();
        LogEntryTable table { get; set; }

        public int CopyCount { get; protected set; }

        public LogEntryTransferManager(AppSettings settings)
        {
            this.settings = settings;
            this.table = new LogEntryTable(props);
            this.CopyCount = 0;
        }

        public async Task ToSql(IEnumerable<LogEntry> entries)
        {
            table.Load(entries);
            CopyCount += entries.Count();

            using (var bc = new SqlBulkCopy(settings.DbConnection))
            {
                bc.DestinationTableName = settings.DbTable;
                foreach (var prop in props)
                {
                    bc.ColumnMappings.Add(prop.Name, prop.Name);
                }

                await bc.WriteToServerAsync(table.Rows);
            }

            table.Clear();
        }
    }
}
