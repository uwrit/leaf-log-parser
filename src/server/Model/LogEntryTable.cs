using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Linq;

namespace Model
{
    class LogEntryTable
    {
        readonly DataTable Table = new DataTable();

        public DataRow[] Rows => Table.Rows.Cast<DataRow>().ToArray();

        IEnumerable<PropertyInfo> Props { get; set; }

        public LogEntryTable(IEnumerable<PropertyInfo> props)
        {
            this.Props = props;
            SetSchema(props);
        }

        public void Add(LogEntry entry)
        {
            var row = Table.NewRow();

            foreach (var prop in Props)
            {
                var val = prop.GetValue(entry);
                row[prop.Name] = val == null || string.IsNullOrWhiteSpace(val.ToString()) ? null : val;
            }

            Table.Rows.Add(row);
        }

        public void Add(IEnumerable<LogEntry> entries)
        {
            foreach (var entry in entries)
            {
                Add(entry);
            }
        }

        public void Clear()
        {
            Table.Rows.Clear();
        }

        void SetSchema(IEnumerable<PropertyInfo> props)
        {
            foreach (var prop in props)
            {
                Table.Columns.Add(new DataColumn(prop.Name, prop.PropertyType) { AllowDBNull = true });
            }
        }
    }
}
