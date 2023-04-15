using abkr.grammarParser;
using Antlr4.Runtime.Misc;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Xml;

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
    public FilterDefinition<BsonDocument> DeleteFilter { get; private set; }


    private readonly XmlDocument metadataXml;

    public MyAbkrGrammarListener(string metadataFilePath)
    {
        metadataXml = new XmlDocument();
        metadataXml.Load(metadataFilePath);

    }


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

            var constraints = columnDefinition.column_constraint();
            foreach (var constraint in constraints)
            {
                if (constraint.PRIMARY() != null && constraint.KEY() != null)
                {
                    PrimaryKeyColumn = columnName;
                }
            }
        }
    }


    public override void EnterColumn_definition(abkr_grammarParser.Column_definitionContext context)
    {
        string columnName = context.identifier().GetText();
        Columns[columnName] = context.data_type().GetText();

        Console.WriteLine($"Column name: {columnName}, Data type: {Columns[columnName]}");

        if (context.column_constraint() != null)
        {
            foreach (var constraint in context.column_constraint())
            {
                if (constraint.PRIMARY() != null)
                {
                    PrimaryKeyColumn = columnName;
                    Console.WriteLine($"Primary key found for column: {PrimaryKeyColumn}");
                    break;
                }
            }
        }
    }





    public override void EnterInsert_statement(abkr_grammarParser.Insert_statementContext context)
    {
        StatementType = StatementType.Insert;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        Columns.Clear();
        Values.Clear();

        int? columnCount = context.identifier_list().identifier().Length;
        if (columnCount != 0)
        {
            for (int i = 0; i < columnCount; i++)
            {
                Columns[context.identifier_list().identifier(i).GetText()] = null;
            }
        }
        int valueCount = context.value_list().value().Length;
        for (int i = 0; i < valueCount; i++)
        {
            Columns[context.identifier_list().identifier(i).GetText()] = context.value_list().value(i).GetText();
        }

        Console.WriteLine("[Insert] Columns: " + string.Join(", ", Columns.Keys));
        Console.WriteLine("[Insert] Values: " + string.Join(", ", Columns.Values));
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

    public override void EnterDelete_statement(abkr_grammarParser.Delete_statementContext context)
    {
        StatementType = StatementType.Delete;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        string columnName = context.identifier(2).GetText();
        string columnValue = context.value().GetText();

        if (columnName == GetPrimaryKeyColumnName(DatabaseName, TableName))
        {
            columnName = "_id";
        }
        else
        {
            columnName = "value." + columnName;
        }

        DeleteFilter = Builders<BsonDocument>.Filter.Eq(columnName, columnValue);
    }




    private string GetPrimaryKeyColumnName(string databaseName, string tableName)
    {
        XmlNode tableNode = metadataXml.SelectSingleNode($"//DataBase[@dataBaseName='{databaseName}']/Table[@tableName='{tableName}']");
        if (tableNode != null)
        {
            XmlNode primaryKeyNode = tableNode.SelectSingleNode("primaryKey");
            if (primaryKeyNode != null)
            {
                return primaryKeyNode.Attributes["name"].Value;
            }
        }

        return null;
    }




    private object GetValueFromValue(abkr_grammarParser.ValueContext context)
    {
        if (context.STRING() != null)
        {
            return context.STRING().GetText().Trim('\'');
        }
        else if (context.NUMBER() != null)
        {
            return int.Parse(context.NUMBER().GetText());
        }

        return null;
    }

}

