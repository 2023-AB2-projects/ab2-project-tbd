using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkrServer.Parser.Listener
{
    public class JoinCondition
    {
        public string DatabaseName { get; set; }
        public string TableAlias { get; set; }
        public string Column1 { get; set; }
        public string Operator { get; set; }
        public string Column2 { get; set; }

        public JoinCondition(string databaseName,string tableAlias, string column1, string column2)
        {
            DatabaseName = databaseName;
            TableAlias = tableAlias;
            Column1 = column1;
            Operator = "=";
            Column2 = column2;
        }
    }
}
