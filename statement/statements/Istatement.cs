namespace abkr.statements
{
    public enum StatementType
    {
        //DDL

        CreateStatement,
        DropStatement,

        //DML
        //DeletStatement,
    }
    public interface IStatement
    {
        StatementType Type { get; set; }
    }
}
