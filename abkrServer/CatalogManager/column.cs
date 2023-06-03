using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkr.CatalogManager
{
    [Serializable]
    public class Column
    {
        public string Name { get; }
        public object Type { get; }  // Assuming you've defined a ColumnType enum
        public bool IsPrimaryKey { get; set; }
        public bool IsUnique { get; set; }
        public ForeignKey? ForeignKeyReference { get; set; }

        public Column(string name, object type, bool isPrimaryKey, bool isUnique, ForeignKey? foreignKeyReference)
        {
            Name = name;
            Type = type;
            IsPrimaryKey = isPrimaryKey;
            IsUnique = isUnique;
            ForeignKeyReference = foreignKeyReference;
        }
    }

}
