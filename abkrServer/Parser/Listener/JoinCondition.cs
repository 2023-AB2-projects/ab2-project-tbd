using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkrServer.Parser.Listener
{
    public class JoinCondition
    {
        public string TableAlias { get; set; }
        public string ConditionColumnName { get; set; }
        public string Operator { get; set; }
        public string ConditionValue { get; set; }

        public JoinCondition(string tableAlias, string conditionColumnName, string op, string conditionValue)
        {
            TableAlias = tableAlias;
            ConditionColumnName = conditionColumnName;
            Operator = op;
            ConditionValue = conditionValue;
        }
    }
}
