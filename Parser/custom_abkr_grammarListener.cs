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
    Insert,
    Delete,
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
    public List<object> Values { get; private set; } = new List<object>();
    public string ColumnName { get; private set; }
    public object ColumnValue { get; private set; }

    public Dictionary<string, object> RowData { get; private set; }
    public string PrimaryKeyColumn { get; private set; }
    public object PrimaryKeyValue { get; private set; }


    // Override the listener methods to extract the required information

    public override void EnterCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
    {
        StatementType = StatementType.CreateDatabase;
        DatabaseName = context.identifier().GetText();
    }

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

            // Check if the current column has a PRIMARY KEY constraint and set PrimaryKeyColumn accordingly
            var constraints = columnDefinition.column_constraint();
            foreach (var constraint in constraints)
            {
                if (constraint.Start.Text == "PRIMARY" && constraint.Stop.Text == "KEY")
                {
                    PrimaryKeyColumn = columnName;
                }
            }
        }
    }
    public override void EnterColumn_constraint(abkr_grammarParser.Column_constraintContext context)
    {
        if (context.PRIMARY() != null && context.KEY() != null)
        {
            PrimaryKeyColumn = ColumnName;
        }
    }

    public override void EnterColumn_definition(abkr_grammarParser.Column_definitionContext context)
    {
        ColumnName = context.identifier().GetText();
        base.EnterColumn_definition(context);
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

    public override void EnterInsert_statement(abkr_grammarParser.Insert_statementContext context)
    {
        StatementType = StatementType.Insert;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        var values = context.value_list().value();
        foreach (var value in values)
        {
            Values.Add(value.GetText());
        }
    }

    public override void EnterDelete_statement(abkr_grammarParser.Delete_statementContext context)
    {
        StatementType = StatementType.Delete;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();
        ColumnName = context.identifier(2).GetText();
        ColumnValue = context.value().GetText();
    }
}

