using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Linq;

namespace Model
{
    class LogEntryTable
    {

        public DataRow[] Rows => table.Rows.Cast<DataRow>().ToArray();

        IEnumerable<PropertyInfo> props { get; set; }

        DataTable table = new DataTable();

        public LogEntryTable(IEnumerable<PropertyInfo> props)
        {
            this.props = props;
            Schema(props);
        }

        public LogEntryTable(IEnumerable<LogEntry> entries, IEnumerable<PropertyInfo> props)
        {
            Schema(props);
            Fill(props, entries);
        }

        public void Load(IEnumerable<LogEntry> entries)
        {
            Fill(props, entries);
        }

        public void Clear()
        {
            table.Rows.Clear();
        }

        void Schema(IEnumerable<PropertyInfo> props)
        {
            foreach (var prop in props)
            {
                table.Columns.Add(new DataColumn(prop.Name, prop.PropertyType) { AllowDBNull = true });
            }
        }

        void Fill(IEnumerable<PropertyInfo> props, IEnumerable<LogEntry> with)
        {
            foreach (var rec in with)
            {
                var row = table.NewRow();

                foreach (var prop in props)
                {
                    var val = prop.GetValue(rec);
                    row[prop.Name] = val; // val == null || string.IsNullOrWhiteSpace(val.ToString()) ? null : val;
                }

                table.Rows.Add(row);
            }
        }
    }
}
