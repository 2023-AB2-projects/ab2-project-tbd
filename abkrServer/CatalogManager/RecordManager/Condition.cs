using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkrServer.CatalogManager.RecordManager
{
    public class Condition
    {
        public string ColumnName { get; set; }
        public string Operator { get; set; }
        public object Value { get; set; }

        public Condition(string columnName, string operator, object value)
        {
            ColumnName = columnName;
            Operator = operator;
            Value = value;
        }
    }

}
