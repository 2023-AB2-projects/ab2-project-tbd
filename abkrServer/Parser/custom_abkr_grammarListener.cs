using abkr.CatalogManager;
using abkr.grammarParser;
using Antlr4.Runtime.Misc;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Xml;
[Serializable]
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
    Select,
    Unknown
}

public class MyAbkrGrammarListener : abkr_grammarBaseListener
{
    [NotNull] public StatementType StatementType { get; private set; } = StatementType.Unknown;
    public string? DatabaseName { get; private set; }
    public string? TableName { get; private set; }
    public string? IndexName { get; private set; }
    public Dictionary<string, object> Columns { get; private set; } = new Dictionary<string, object>();
    public BsonArray IndexColumns { get; private set; } = new BsonArray();
    public List<object> Values { get; private set; } = new List<object>();
    public string? ColumnName { get; private set; }
    public object ColumnValue { get; private set; }
    public Dictionary<string, object> RowData { get; private set; }
    public string PrimaryKeyColumn { get; private set; }
    public object PrimaryKeyValue { get; private set; }
    public FilterDefinition<BsonDocument> DeleteFilter { get; private set; }
    public List<ForeignKey> ForeignKeyColumns { get; private set; } = new List<ForeignKey>();
    public List<string> UniqueKeyColumns { get; private set; } = new List<string>();
    public string[] SelectedColumns { get; private set; }
    public string SelectCondition { get; private set; }
    public FilterDefinition<BsonDocument> SelectFilter { get; private set; }
    public string ForeignColumn { get; private set; }

    private CatalogManager CatalogManager { get; set; }




    private readonly XmlDocument metadataXml;

    public MyAbkrGrammarListener(string metadataFilePath)
    {
        CatalogManager = new CatalogManager(metadataFilePath);
        metadataXml = new XmlDocument();
        try
        {
            metadataXml.Load(metadataFilePath);
        }
        catch (XmlException ex)
        {
            Console.WriteLine("Invalid XML. Details: " + ex.Message);
        }
        catch (IOException ex)
        {
            Console.WriteLine("Problem reading file. Details: " + ex.Message);
        }
    }

    // Override the listener methods to extract the required information

    public override void EnterCreate_database_statement(abkr_grammar.Create_database_statementContext context)
    {
        StatementType = StatementType.CreateDatabase;
        DatabaseName = context.identifier().GetText();
    }

    public override void EnterDrop_database_statement([NotNull] abkr_grammar.Drop_database_statementContext context)
    {
        StatementType = StatementType.DropDatabase;
        DatabaseName = context.identifier().GetText();
    }
    public override void EnterCreate_table_statement(abkr_grammar.Create_table_statementContext context)
    {
        StatementType = StatementType.CreateTable;
        SetDatabaseAndTableName(context);
        ProcessColumnDefinitions(context.column_definition_list().column_definition());
    }

    private void SetDatabaseAndTableName(abkr_grammar.Create_table_statementContext context)
    {
        DatabaseName = context.identifier()[0].GetText();
        TableName = context.identifier()[1].GetText();
    }

    private void ProcessColumnDefinitions(IEnumerable<abkr_grammar.Column_definitionContext> columnDefinitions)
    {
        Columns = new Dictionary<string, object>();
        ForeignKeyColumns = new List<ForeignKey>();
        UniqueKeyColumns = new List<string>();

        foreach (var columnDefinition in columnDefinitions)
        {
            AddColumn(columnDefinition);
            ProcessColumnConstraints(columnDefinition.column_constraint());
        }
    }

    private void AddColumn(abkr_grammar.Column_definitionContext columnDefinition)
    {
        var columnName = columnDefinition.identifier().GetText();
        ColumnName = columnName;
        var columnType = columnDefinition.data_type().GetText();
        Columns[columnName] = columnType;
    }

    private void ProcessColumnConstraints(IEnumerable<abkr_grammar.Column_constraintContext> columnConstraints)
    {
        if (columnConstraints == null)
        {
            return;
        }

        foreach (var constraint in columnConstraints)
        {
            ProcessConstraint(constraint);
        }
    }

    private void ProcessConstraint(abkr_grammar.Column_constraintContext constraint)
    {

        if (IsPrimaryKey(constraint))
        {
            AddPrimaryKey(ColumnName);
        }

        if (IsForeignKey(constraint))
        {
            AddForeignKey(constraint, ColumnName);
        }

        if (IsUniqueKey(constraint))
        {
            AddUniqueKey(ColumnName);
        }
    }

    private bool IsPrimaryKey(abkr_grammar.Column_constraintContext constraint)
    {
        return constraint.PRIMARY() != null;
    }

    private void AddPrimaryKey(string columnName)
    {
        PrimaryKeyColumn = columnName;
        UniqueKeyColumns.Add(columnName);
    }

    private bool IsForeignKey(abkr_grammar.Column_constraintContext constraint)
    {
        return constraint.FOREIGN() != null && constraint.identifier().Length > 2;
    }

    private void AddForeignKey(abkr_grammar.Column_constraintContext constraint, string columnName)
    {
        var databaseName = constraint.identifier()[0].GetText();
        var foreignTable = constraint.identifier()[1].GetText();
        var foreignAlias = constraint.identifier()[2].GetText();
        var isUnique = CatalogManager.IsForeignKeyUnique(databaseName, foreignTable, foreignAlias);
        var fk = new ForeignKey(TableName, columnName, foreignTable, foreignAlias, isUnique );
        ForeignKeyColumns.Add(fk);
    }


    private bool IsUniqueKey(abkr_grammar.Column_constraintContext constraint)
    {
        return constraint.UNIQUE() != null;
    }

    private void AddUniqueKey(string columnName)
    {
        UniqueKeyColumns.Add(columnName);
    }



    public override void EnterInsert_statement(abkr_grammar.Insert_statementContext context)
    {
        StatementType = StatementType.Insert;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        Columns.Clear();
        int valueCount = context.value_list().value().Length;
        int columnCount = context.identifier_list().identifier().Length;

        if (columnCount != valueCount)
        {
            throw new Exception("Mismatch between column count and value count");
        }

        for (int i = 0; i < columnCount; i++)
        {
            Columns[context.identifier_list().identifier(i).GetText()] = context.value_list().value(i).GetText();
        }

        Console.WriteLine("[Insert] Columns: " + string.Join(", ", Columns.Keys));
        Console.WriteLine("[Insert] Values: " + string.Join(", ", Columns.Values));
    }



    public override void EnterDrop_table_statement(abkr_grammar.Drop_table_statementContext context)
    {
        StatementType = StatementType.DropTable;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();
    }


    public override void EnterCreate_index_statement(abkr_grammar.Create_index_statementContext context)
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

    public override void EnterDrop_index_statement(abkr_grammar.Drop_index_statementContext context)
    {
        StatementType = StatementType.DropIndex;
        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();
        IndexName = context.identifier(2).GetText();
    }

    public override void EnterDelete_statement(abkr_grammar.Delete_statementContext context)
    {
        StatementType = StatementType.Delete;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        var whereClauseContext = context.where_clause();
        if (whereClauseContext != null)
        {
            var fieldName = whereClauseContext.condition().identifier().GetText();
            var operatorText = whereClauseContext.condition().comparison_operator().GetText();
            var value = ExtractValue(whereClauseContext.condition().value());

            DeleteFilter = CreateFilter(fieldName, operatorText, value);
        }
    }

    private FilterDefinition<BsonDocument> CreateFilter(string fieldName, string operatorText, object value)
    {
        return operatorText switch
        {
            "EQUALS" => Builders<BsonDocument>.Filter.Eq(fieldName, value),
            "GREATER_THAN" => Builders<BsonDocument>.Filter.Gt(fieldName, value),
            "GREATER_EQUALS" => Builders<BsonDocument>.Filter.Gte(fieldName, value),
            "LESS_THAN" => Builders<BsonDocument>.Filter.Lt(fieldName, value),
            "LESS_EQUALS" => Builders<BsonDocument>.Filter.Lte(fieldName, value),
            _ => throw new InvalidOperationException($"Unknown operator '{operatorText}'")
        };
    }


    public override void EnterSelect_statement(abkr_grammar.Select_statementContext context)
    {
        StatementType = StatementType.Select;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        var columnListContext = context.column_list();
        if (columnListContext.ASTERISK() != null)
        {
            SelectedColumns = new string[0];
        }
        else if (columnListContext.identifier_list() != null)
        {
            SelectedColumns = columnListContext.identifier_list().identifier().Select(c => c.GetText()).ToArray();
        }

        var whereClauseContext = context.where_clause();
        if (whereClauseContext != null)
        {
            var fieldName = whereClauseContext.condition().identifier().GetText();
            var operatorText = whereClauseContext.condition().comparison_operator().GetText();
            var value = ExtractValue(whereClauseContext.condition().value());

            SelectFilter = CreateFilter(fieldName, operatorText, value);
        }
    }


    private static object ExtractValue(abkr_grammar.ValueContext context)
    {
        // You extract the value here based on its type
        if (context.NUMBER() != null)
            return int.Parse(context.NUMBER().GetText());

        if (context.STRING() != null)
            return context.STRING().GetText();

        // Add cases for other types as well
        return null;
    }

    public override void EnterWhere_clause(abkr_grammar.Where_clauseContext context)
    {
        var identifier = context.condition().identifier().GetText();
        var comparisonOperator = context.condition().comparison_operator().GetText();
        var value = GetValueFromValue(context.condition().value());

        if (identifier == GetPrimaryKeyColumnName(DatabaseName, TableName))
        {
            identifier = "_id";
        }

        var filter = CreateFilter(identifier, comparisonOperator, value);

        switch (StatementType)
        {
            case StatementType.Delete:
                DeleteFilter = filter;
                break;
            case StatementType.Select:
                SelectFilter = filter;
                break;
        }
    }


    private string GetPrimaryKeyColumnName(string databaseName, string tableName)
    {
        var tableNode = metadataXml.SelectSingleNode($"//DataBase[@dataBaseName='{databaseName}']/Table[@tableName='{tableName}']")
                       ?? throw new InvalidOperationException($"Table {tableName} not found in database {databaseName}");

        var primaryKeyNode = tableNode.SelectSingleNode("primaryKey")
                             ?? throw new InvalidOperationException($"No primary key found for table {tableName}");

        return primaryKeyNode.Attributes["name"].Value;
    }

    private  object GetValueFromValue(abkr_grammar.ValueContext context)
    {
        return context.STRING() != null
            ? context.STRING().GetText().Trim('\'')
            : context.NUMBER() != null
                ? int.Parse(context.NUMBER().GetText())
                : null;
    }


}
