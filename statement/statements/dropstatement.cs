namespace abkr.statements
{
    public enum DropTarget
    {
        Database,
        Table,
        Index,
    }

    public class DropStatement : IStatement
    {
        public StatementType Type { get; set; } = StatementType.DropStatement;
        public DropTarget TargetType { get; set; }
        public string DatabaseName { get; set; }
        // leave blank if drop table
        public string IndexName { get; set; } = "";
        // leave blank if drop index does not specify any table
        public string TableName { get; set; } = "";
    }
}