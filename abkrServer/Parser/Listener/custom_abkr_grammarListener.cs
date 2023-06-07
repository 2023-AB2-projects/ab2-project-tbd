using abkr.CatalogManager;
using abkr.ServerLogger;
using abkrServer.CatalogManager.RecordManager;
using abkrServer.Parser.Listener;
using Amazon.Auth.AccessControlPolicy;
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
    public List<ForeignKey> ForeignKeyColumns { get; private set; } = new List<ForeignKey>();
    public List<string> UniqueKeyColumns { get; private set; } = new List<string>();
    public string[] SelectedColumns { get; private set; }
    public string SelectCondition { get; private set; }
    public string ForeignColumn { get; private set; }

    private CatalogManager CatalogManager { get; set; }
    private Logger Logger { get; set; }
    public string Operator { get; set; }
    public List<FilterCondition> Conditions { get; private set; } = new List<FilterCondition>();
    public List<JoinCondition> JoinConditions { get; private set; } = new List<JoinCondition>();







    private readonly XmlDocument metadataXml;

    public MyAbkrGrammarListener(string metadataFilePath, CatalogManager catalogManager, Logger logger)
    {
        Logger = logger;
        CatalogManager = catalogManager;
        Console.WriteLine("\n" + metadataFilePath + "\n");
        metadataXml = new XmlDocument();
        try
        {
            metadataXml.Load(metadataFilePath);
        }
        catch (XmlException ex)
        {
            Logger.LogMessage("Invalid XML. Details: " + ex.Message);
        }
        catch (IOException ex)
        {
            Logger.LogMessage("Problem reading file. Details: " + ex.Message);
        }
    }

    // Override the listener methods to extract the required information

    public override void EnterCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
    {
        StatementType = StatementType.CreateDatabase;
        DatabaseName = context.identifier().GetText();
    }

    public override void EnterDrop_database_statement([NotNull] abkr_grammarParser.Drop_database_statementContext context)
    {
        StatementType = StatementType.DropDatabase;
        DatabaseName = context.identifier().GetText();
    }
    public override void EnterCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
    {
        StatementType = StatementType.CreateTable;
        SetDatabaseAndTableName(context);
        ProcessColumnDefinitions(context.column_definition_list().column_definition());
    }

    private void SetDatabaseAndTableName(abkr_grammarParser.Create_table_statementContext context)
    {
        DatabaseName = context.identifier()[0].GetText();
        TableName = context.identifier()[1].GetText();
    }

    private void ProcessColumnDefinitions(IEnumerable<abkr_grammarParser.Column_definitionContext> columnDefinitions)
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

    private void AddColumn(abkr_grammarParser.Column_definitionContext columnDefinition)
    {
        var columnName = columnDefinition.identifier().GetText();
        ColumnName = columnName;
        var columnType = columnDefinition.data_type().GetText();
        Columns[columnName] = columnType;
    }

    private void ProcessColumnConstraints(IEnumerable<abkr_grammarParser.Column_constraintContext> columnConstraints)
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

    private void ProcessConstraint(abkr_grammarParser.Column_constraintContext constraint)
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

    private static bool IsPrimaryKey(abkr_grammarParser.Column_constraintContext constraint)
    {
        return constraint.PRIMARY() != null;
    }

    private void AddPrimaryKey(string columnName)
    {
        PrimaryKeyColumn = columnName;
        UniqueKeyColumns.Add(columnName);
    }

    private static bool IsForeignKey(abkr_grammarParser.Column_constraintContext constraint)
    {
        return constraint.FOREIGN() != null && constraint.identifier().Length > 2;
    }

    private void AddForeignKey(abkr_grammarParser.Column_constraintContext constraint, string columnName)
    {
        var databaseName = constraint.identifier()[0].GetText();
        var foreignTable = constraint.identifier()[1].GetText();
        var foreignAlias = constraint.identifier()[2].GetText();
        var isUnique = CatalogManager.IsForeignKeyUnique(databaseName, foreignTable, foreignAlias);

        var pk = CatalogManager.GetPrimaryKeyColumn(databaseName, foreignTable);
        var ispk = pk == foreignAlias;

        var fk = new ForeignKey(TableName, columnName, foreignTable, (foreignAlias, ispk), isUnique);
        ForeignKeyColumns.Add(fk);
    }


    private static bool IsUniqueKey(abkr_grammarParser.Column_constraintContext constraint)
    {
        return constraint.UNIQUE() != null;
    }

    private void AddUniqueKey(string columnName)
    {
        UniqueKeyColumns.Add(columnName);
    }



    public override void EnterInsert_statement(abkr_grammarParser.Insert_statementContext context)
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

        //Logger.LogMessage("[Insert] Columns: " + string.Join(", ", Columns.Keys));
        //Logger.LogMessage("[Insert] Values: " + string.Join(", ", Columns.Values));
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
        Conditions.Clear(); // Clear the conditions for a new statement
    }

    public override void EnterJoin_clause([NotNull] abkr_grammarParser.Join_clauseContext context)
    {
        var databaseName = context.identifier(0)?.GetText();
        var tableName = context.identifier(1)?.GetText();
        var columnName1 = context.identifier(2)?.GetText();
        var columnName2 = context.identifier(3)?.GetText();

        JoinConditions.Add(new JoinCondition(databaseName, tableName, columnName1, columnName2));
    }



    public override void EnterSelect_statement(abkr_grammarParser.Select_statementContext context)
    {
        StatementType = StatementType.Select;
        DatabaseName = context.table_source().identifier(0).GetText();
        TableName = context.table_source().identifier(1).GetText();
        Conditions.Clear(); // Clear the conditions for a new statement

        var columnListContext = context.column_list();
        if (columnListContext.ASTERISK() != null)
        {
            SelectedColumns = new string[0];
        }
        else if (columnListContext.identifier_list() != null)
        {
            SelectedColumns = columnListContext.identifier_list().identifier().Select(c => c.GetText()).ToArray();
        }

        // Handle the JOIN clauses
        JoinConditions.Clear();  // Clear the join conditions for a new statement
        var joinClauses = context.table_source().join_clause();
        if (joinClauses != null)
        {
            foreach (var joinClause in joinClauses)
            {
                EnterJoin_clause(joinClause);
            }
        }

        var whereClauseContext = context.where_clause();
        if (whereClauseContext != null)
        {
            EnterExpression(whereClauseContext.condition().expression());
        }
    }

    public override void EnterSimpleCondition([NotNull] abkr_grammarParser.SimpleConditionContext context)
    {
        var columnName = context.identifier().GetText();
        var op = context.comparison_operator().GetText();
        var value = ExtractValue(context.value())
            ?? throw new NullReferenceException();

        Conditions.Add(new FilterCondition(columnName, op, value.ToString()));
    }

    public override void EnterAndExpression([NotNull] abkr_grammarParser.AndExpressionContext context)
    {
        // Here, both sides of AND are expressions themselves,
        // you need to recursively handle them
        var leftExpression = context.expression(0);
        var rightExpression = context.expression(1);

        // For example:
        EnterExpression(leftExpression);
        EnterExpression(rightExpression);
    }

    public override void EnterParenExpression([NotNull] abkr_grammarParser.ParenExpressionContext context)
    {
        var innerExpression = context.expression();
        EnterExpression(innerExpression);
    }

    private void EnterExpression(abkr_grammarParser.ExpressionContext context)
    {
        // Depending on the specific type of the expression,
        // call the appropriate method.
        if (context is abkr_grammarParser.SimpleConditionContext simpleConditionContext)
        {
            EnterSimpleCondition(simpleConditionContext);
        }
        else if (context is abkr_grammarParser.AndExpressionContext andExpressionContext)
        {
            EnterAndExpression(andExpressionContext);
        }
        else if (context is abkr_grammarParser.ParenExpressionContext parenExpressionContext)
        {
            EnterParenExpression(parenExpressionContext);
        }
    }


    // Don't forget to clear the conditions list when entering a new statement:

    public override void EnterStatement([NotNull] abkr_grammarParser.StatementContext context)
    {
        Conditions.Clear();
    }



    private static object ExtractValue(abkr_grammarParser.ValueContext context)
    {
        // You extract the value here based on its type
        if (context.NUMBER() != null)
            return int.Parse(context.NUMBER().GetText());

        if (context.STRING() != null)
            return context.STRING().GetText();

        // Add cases for other types as well
        return null;
    }
}
