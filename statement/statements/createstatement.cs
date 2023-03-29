//namespace abkr.statements
//{
//    public enum CreateType
//    {
//        Database,
//        Table,
//        Index,
//    }

//    [Serializable]
//    public class CreateStatement : IStatement
//    {
//        public StatementType Type { get; set; } = StatementType.CreateStatement;
//        public CreateType CreateType { get; set; }
//        public string DatabaseName { get; set; }
//        public string TableName { get; set; }
//        // create index only
//        public bool IsUnique { get; set; } = false;
//        public string IndexName { get; set; }
//        public List<string> Columns { get; set; } // For creating index
//        // create table only
//        // it is assigned to "" if PrimaryKey does not exist
//        private string primaryKey = "";
//        public string PrimaryKey
//        {
//            get { return primaryKey; }
//            set
//            {
//                if (this.AttributeDeclarations.Find(x => x.AttributeName == value) == null)
//                    throw new System.Exception($"Primary key {value} does not exist");
//                primaryKey = value;
//            }
//        }
//        public List<AttributeDeclaration> AttributeDeclarations { get; set; }
//        // for serialization
//        public CreateStatement() { }
//    }
//}