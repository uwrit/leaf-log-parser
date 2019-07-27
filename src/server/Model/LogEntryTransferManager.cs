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
        readonly IEnumerable<PropertyInfo> Props = typeof(LogEntry).GetProperties();

        AppSettings Settings { get; set; }

        LogEntryTable Table { get; set; }

        public int CopiedCount { get; protected set; }

        public int RowCount => Table.Rows.Length;

        public LogEntryTransferManager(AppSettings settings)
        {
            this.Settings = settings;
            this.Table = new LogEntryTable(Props);
            this.CopiedCount = 0;
        }

        public void Add(LogEntry entry)
        {
            Table.Add(entry);
        }

        public async Task ToSql()
        {
            CopiedCount += RowCount;

            using (var bc = new SqlBulkCopy(Settings.DbConnection))
            {
                bc.DestinationTableName = Settings.DbTable;
                foreach (var prop in Props)
                {
                    bc.ColumnMappings.Add(prop.Name, prop.Name);
                }

                await bc.WriteToServerAsync(Table.Rows);
            }

            Table.Clear();
        }
    }
}
