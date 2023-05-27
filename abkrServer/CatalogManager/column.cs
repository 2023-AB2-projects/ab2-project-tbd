using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace abkr.CatalogManager
{
    public class Column
    {
        public string? Name { get; set; }
        public object ?Type { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsUnique { get; set; }
        public string? ForeignKeyReference { get; set; } // Name of the foreign key reference if it exists
    }

}
