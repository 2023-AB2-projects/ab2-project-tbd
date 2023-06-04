using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkrServer.CatalogManager.RecordManager
{
    public class FilterCondition
    {
        public string ColumnName { get; set; }
        public string Operator { get; set; }
        public object Value { get; set; }

        public FilterCondition(string columnName, string op, object value)
        {
            ColumnName = columnName;
            Operator = op;
            Value = value;
        }
    }

}
