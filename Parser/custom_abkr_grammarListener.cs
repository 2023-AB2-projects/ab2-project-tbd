using abkr.grammarParser;
using Antlr4.Runtime.Misc;
using MongoDB.Bson;

public enum StatementType
{
    CreateDatabase,
    CreateTable,
    DropDatabase,
    DropTable,
    CreateIndex,
    DropIndex,
    Unknown
}

public class MyAbkrGrammarListener : abkr_grammarBaseListener
{
    public StatementType StatementType { get; private set; } = StatementType.Unknown;
    public string DatabaseName { get; private set; }
    public string TableName { get; private set; }
    public string IndexName { get; private set; }
    public Dictionary<string, object> Columns { get; private set; } = new Dictionary<string, object>();
    public BsonArray IndexColumns { get; private set; } = new BsonArray();

    // Override the listener methods to extract the required information

    public override void EnterCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
    {
        StatementType = StatementType.CreateDatabase;
        DatabaseName = context.identifier().GetText();
    }

    // Implement similar methods for other

    public override void EnterCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
    {
        StatementType = StatementType.CreateTable;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        var columnDefinitions = context.column_definition_list().column_definition();
        foreach (var columnDefinition in columnDefinitions)
        {
            var columnName = columnDefinition.identifier().GetText();
            var dataType = columnDefinition.data_type().GetText();
            Columns[columnName] = dataType;
        }
    }


    public override void EnterDrop_table_statement(abkr_grammarParser.Drop_table_statementContext context)
    {
        StatementType = StatementType.DropTable;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();
    }


    public override void EnterCreate_index_statement(abkr_grammarParser.Create_index_statementContext context)
        {
            StatementType = StatementType.CreateIndex;
            DatabaseName = context.identifier(0).GetText();
            TableName = context.identifier(1).GetText();
            IndexName = context.identifier(2).GetText();

            var columnIdentifiers = context.identifier_list().identifier();
            foreach (var columnIdentifier in columnIdentifiers)
            {
                IndexColumns.Add(columnIdentifier.GetText());
            }
        }

        public override void EnterDrop_index_statement(abkr_grammarParser.Drop_index_statementContext context)
        {
            StatementType = StatementType.DropIndex;
            DatabaseName = context.identifier(0).GetText();
            TableName = context.identifier(1).GetText();
            IndexName = context.identifier(2).GetText();
        }
    }

