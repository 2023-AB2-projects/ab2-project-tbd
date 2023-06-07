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
        public string ConditionColumnName { get; set; }
        public string Operator { get; set; }
        public string ConditionValue { get; set; }

        public JoinCondition(string databaseName, string tableAlias, string conditionColumnName, string conditionValue)
        {
            DatabaseName = databaseName;
            TableAlias = tableAlias;
            ConditionColumnName = conditionColumnName;
            Operator = "=";
            ConditionValue = conditionValue;
        }
    }
}
