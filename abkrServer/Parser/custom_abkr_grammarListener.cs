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
    [NotNull]public StatementType StatementType { get; private set; } = StatementType.Unknown;
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
    public Dictionary<string, string> ForeignKeyColumns { get; private set; } = new Dictionary<string, string>();
    public List<string> UniqueKeyColumns { get; private set; } = new List<string>();

    public bool IsUnique = false;
    public string[] SelectedColumns { get; private set; }
    public string SelectCondition { get; private set; }
    public FilterDefinition<BsonDocument> SelectFilter { get; private set; }
    public string ForeignColumn { get; private set; }




    private readonly XmlDocument metadataXml;

    public MyAbkrGrammarListener(string metadataFilePath)
    {
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
        DatabaseName = context.identifier()[0].GetText();
        TableName = context.identifier()[1].GetText();
        var columnDefinitions = context.column_definition_list().column_definition();
        Columns = new Dictionary<string, object>();
        ForeignKeyColumns = new Dictionary<string, string>();
        UniqueKeyColumns = new List<string>();

        foreach (var columnDefinition in columnDefinitions)
        {
            var columnName = columnDefinition.identifier().GetText();
            var columnType = columnDefinition.data_type().GetText();
            Columns[columnName] = columnType;

            var columnConstraints = columnDefinition.column_constraint();
            if (columnConstraints.Any())  // Ensure there are constraints before iterating
            {
                foreach (var constraint in columnConstraints)
                {
                    if (constraint.UNIQUE() != null)
                    {
                        UniqueKeyColumns.Add(columnName);
                    }
                    else if (constraint.PRIMARY() != null)
                    {
                        PrimaryKeyColumn = columnName;
                        UniqueKeyColumns.Add(columnName);
                    }
                    else if (constraint.FOREIGN() != null && constraint.identifier().Length >= 2)
                    {
                        var foreignTable = constraint.identifier()[0].GetText();
                        var foreignColumn = constraint.identifier()[1].GetText();
                        ForeignKeyColumns[columnName] = $"{foreignTable}.{foreignColumn}";
                    }
                    else if (constraint.FOREIGN() != null)
                    {
                        ForeignKeyColumns[columnName] = constraint.identifier()[0].GetText();
                    }
                }
            }
        }
    }

    public override void EnterColumn_definition(abkr_grammar.Column_definitionContext context)
    {
        ColumnName = context.identifier().GetText();
        Columns[ColumnName] = context.data_type().GetText();

        Console.WriteLine($"Column name: {ColumnName}, Data type: {Columns[ColumnName]}");

        if (context.column_constraint() != null)
        {
            foreach (var constraint in context.column_constraint())
            {
                // Handle primary keys
                if (constraint.PRIMARY() != null)
                {             
                    PrimaryKeyColumn = ColumnName;
                    Console.WriteLine($"Primary key found for column: {PrimaryKeyColumn}");
                    break;  // Once primary key is found, no need to check for other constraints
                }

                // Handle foreign keys
                if (constraint.FOREIGN() != null)
                {
                    string? databaseName = constraint.identifier(0)?.GetText();
                    string? foreignTable = constraint.identifier(1)?.GetText();
                    string? foreignColumn = constraint.identifier(2)?.GetText();
                    if (!string.IsNullOrEmpty(databaseName) && !string.IsNullOrEmpty(foreignTable) && !string.IsNullOrEmpty(foreignColumn))
                    {
                        ForeignKeyColumns[ColumnName] = $"{databaseName}.{foreignTable}";
                        ForeignColumn= foreignColumn;
                        Console.WriteLine($"Foreign key found for column: {ColumnName} referencing: {ForeignKeyColumns[ColumnName]}:{ForeignColumn}");
                    }
                }

                // Handle unique keys
                if (constraint.UNIQUE() != null && !string.IsNullOrEmpty(ColumnName))
                {
                    UniqueKeyColumns.Add(ColumnName);
                    Console.WriteLine($"Unique key found for column: {ColumnName}");
                }
            }
        }
    }


    public override void EnterInsert_statement(abkr_grammar.Insert_statementContext context)
    {
        StatementType = StatementType.Insert;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        Columns.Clear();
        Values.Clear();
        int valueCount = context.value_list().value().Length;

        int? columnCount = context.identifier_list().identifier().Length;
        if (columnCount != null && columnCount == valueCount)
        {
            for (int i = 0; i < columnCount; i++)
            {
                Columns[context.identifier_list().identifier(i).GetText()] = context.value_list().value(i).GetText();
            }
        }
        else
        {
            throw new Exception("Mismatch between column count and value count");
        }
        for (int i = 0; i < valueCount; i++)
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

        // You get the where clause context here
        var whereClauseContext = context.where_clause();
        if (whereClauseContext != null)
        {
            // Now you can extract information from the where clause using its context
            var fieldName = whereClauseContext.condition().identifier().GetText();
            var operatorText = whereClauseContext.condition().comparison_operator().GetText();
            var value = ExtractValue(whereClauseContext.condition().value());

            // Based on the operator, you can form the FilterDefinition for delete operation
            if (operatorText == "EQUALS")
                DeleteFilter = Builders<BsonDocument>.Filter.Eq(fieldName, value);

            switch (operatorText)
            {
                case "EQUALS":
                    DeleteFilter = Builders<BsonDocument>.Filter.Eq(fieldName, value);
                    break;
                case "GREATER_THAN":
                    DeleteFilter = Builders<BsonDocument>.Filter.Gt(fieldName, value);
                    break;
                case "GREATER_EQUALS":
                    DeleteFilter = Builders<BsonDocument>.Filter.Gte(fieldName, value);
                    break;
                case "LESS_THAN":
                    DeleteFilter = Builders<BsonDocument>.Filter.Lt(fieldName, value);
                    break;
                case "LESS_EQUALS":
                    DeleteFilter = Builders<BsonDocument>.Filter.Lte(fieldName, value);
                    break;
            }
        }
    }


    public override void EnterSelect_statement(abkr_grammar.Select_statementContext context)
    {
        StatementType = StatementType.Select;

        DatabaseName = context.identifier(0).GetText();
        TableName = context.identifier(1).GetText();

        // Handle column list
        var columnListContext = context.column_list();
        if (columnListContext.ASTERISK() != null)
        {
            // Select all columns
            SelectedColumns = new string[0];  // Or any indicator of "all columns"
        }
        else if (columnListContext.identifier_list() != null)
        {
            var columnIdentifiers = columnListContext.identifier_list().identifier();
            SelectedColumns = columnIdentifiers.Select(c => c.GetText()).ToArray();
        }

        // You get the where clause context here
        var whereClauseContext = context.where_clause();
        if (whereClauseContext != null)
        {
            // Now you can extract information from the where clause using its context
            var fieldName = whereClauseContext.condition().identifier().GetText();
            var operatorText = whereClauseContext.condition().comparison_operator().GetText();
            var value = ExtractValue(whereClauseContext.condition().value());

            // Based on the operator, you can form the FilterDefinition for select 

            switch (operatorText)
            {
                case "EQUALS":
                    SelectFilter = Builders<BsonDocument>.Filter.Eq(fieldName, value);
                    break;
                case "GREATER_THAN":
                    SelectFilter = Builders<BsonDocument>.Filter.Gt(fieldName, value);
                    break;
                case "GREATER_EQUALS":
                    SelectFilter = Builders<BsonDocument>.Filter.Gte(fieldName, value);
                    break;
                case "LESS_THAN":
                    SelectFilter = Builders<BsonDocument>.Filter.Lt(fieldName, value);
                    break;
                case "LESS_EQUALS":
                    SelectFilter = Builders<BsonDocument>.Filter.Lte(fieldName, value);
                    break;
            }
        }
    }

    private object ExtractValue(abkr_grammar.ValueContext context)
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

        var builder = Builders<BsonDocument>.Filter;

        if (identifier == GetPrimaryKeyColumnName(DatabaseName, TableName))
        {
            identifier = "_id";
        }

        switch (comparisonOperator)
        {
            case "EQUALS":
                DeleteFilter = builder.Eq(identifier, value);
                SelectFilter = builder.Eq(identifier, value);
                break;
            case "GREATER_THAN":
                DeleteFilter = builder.Gt(identifier, value);
                SelectFilter = builder.Gt(identifier, value);
                break;
            case "GREATER_EQUALS":
                DeleteFilter = builder.Gte(identifier, value);
                SelectFilter = builder.Gte(identifier, value);
                break;
            case "LESS_THAN":
                DeleteFilter = builder.Lt(identifier, value);
                SelectFilter = builder.Lt(identifier, value);
                break;
            case "LESS_EQUALS":
                DeleteFilter = builder.Lte(identifier, value);
                SelectFilter = builder.Lte(identifier, value);
                break;
        }
    }


    private string GetPrimaryKeyColumnName(string databaseName, string tableName)
    {
        XmlNode? tableNode = metadataXml.SelectSingleNode($"//DataBase[@dataBaseName='{databaseName}']/Table[@tableName='{tableName}']");
        if (tableNode != null)
        {
            XmlNode? primaryKeyNode = tableNode.SelectSingleNode("primaryKey");
            if (primaryKeyNode != null)
            {
                return primaryKeyNode.Attributes["name"].Value;
            }
        }

        return null;
    }




    private object GetValueFromValue(abkr_grammar.ValueContext context)
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

