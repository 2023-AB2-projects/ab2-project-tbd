using MongoDB.Driver;
using MongoDB.Bson;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using System.Xml.Linq;
using System.Diagnostics.CodeAnalysis;
using abkrServer.CatalogManager.RecordManager;
using abkr.ServerLogger;
using System.Text;
using abkrServer.Parser.Listener;
using MongoDB.Bson.Serialization.IdGenerators;

namespace abkr.CatalogManager
{

    public class DatabaseServer
    {
        static private IMongoClient _client;
        static public IMongoClient MongoClient => _client;
        static private CatalogManager _catalogManager;
        static private Logger logger;
        public static string LastQueryResult { get; private set; }


        public DatabaseServer(string connectionString, string metadataFileName, Logger logger)
        {
            DatabaseServer.logger = logger;
            _client = new MongoClient(connectionString);
            string metadataFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, metadataFileName);

            if (!File.Exists(metadataFilePath))
            {
                XElement emptyMetadata = new XElement("metadata");
                File.WriteAllText(metadataFilePath, emptyMetadata.ToString() + "\n");
            }

            _catalogManager = new CatalogManager(metadataFilePath, logger);
        }

        public static Task ExecuteStatementAsync(string sql)
        {
            logger.LogMessage("ExecuteStatementAsync called");

            // Create a new instance of the ANTLR input stream with the SQL statement
            var inputStream = new AntlrInputStream(sql);

            // Create a new instance of the lexer and pass the input stream
            var lexer = new abkr_grammarLexer(inputStream);

            // Create a new instance of the common token stream and pass the lexer
            var tokenStream = new CommonTokenStream(lexer);

            // Create a new instance of the parser and pass the token stream
            var parser = new abkr_grammarParser(tokenStream);

            // Invoke the parser's entry rule (statement) and get the parse tree
            var parseTree = parser.statement();

            var listener = new MyAbkrGrammarListener("C:/Users/bfcsa/source/repos/abkr/abkrServer/Parser/example.xml",_catalogManager, logger);
            ParseTreeWalker.Default.Walk(listener, parseTree);

            // Perform actions based on the parsed statement
            if (listener.StatementType == StatementType.CreateDatabase)
            {
                RecordManager.CreateDatabase(listener.DatabaseName, _client, _catalogManager);
            }
            else if (listener.StatementType == StatementType.CreateTable)
            {
                var columns = new List<Column>();
                foreach (var column in listener.Columns)
                {
                    var columnName = column.Key;
                    var columnType = column.Value;
                    var isUnique = listener.UniqueKeyColumns.Contains(columnName);
                    var isPrimaryKey = columnName == listener.PrimaryKeyColumn;

                    logger.LogMessage($"DatabaseServer.ExecuteStatement: Column {columnName} is a {columnType} {(isPrimaryKey ? "primary key" : "")}");

                    var foreignKeyReference = listener.ForeignKeyColumns.Where(fk => fk.ColumnName == columnName).FirstOrDefault();
                    if (foreignKeyReference != null)
                    {
                        logger.LogMessage($"DatabaseServer.ExecuteStatement: Column {columnName} is a foreign key reference to {foreignKeyReference}");
                    }


                    if (isPrimaryKey)
                    {
                        isUnique = true;
                    }

                    logger.LogMessage($"DatabaseServer.ExecuteStatement: Column {columnName} is a {columnType} {(isPrimaryKey ? "primary key" : "")} {(isUnique ? "unique" : "")} {(foreignKeyReference != null ? $"foreign key reference to {foreignKeyReference}" : "")}");

                    var newColumn = new Column(columnName, columnType, isPrimaryKey, isUnique, foreignKeyReference);

                    columns.Add(newColumn);
                }

                RecordManager.CreateTable(listener.DatabaseName, listener.TableName, columns, _catalogManager, _client);
            }

            else if (listener.StatementType == StatementType.DropDatabase)
            {
                RecordManager.DropDatabase(listener.DatabaseName, _client, _catalogManager);
            }
            else if (listener.StatementType == StatementType.DropTable)
            {
                RecordManager.DropTable(listener.DatabaseName, listener.TableName, _client, _catalogManager);
            }
            else if (listener.StatementType == StatementType.CreateIndex)
            {
                RecordManager.CreateIndex(listener.DatabaseName, listener.TableName, listener.IndexName, listener.IndexColumns, _catalogManager.IsUniqueKey(listener.DatabaseName, listener.TableName, listener.IndexColumns[0]), _client, _catalogManager);
            }
            else if (listener.StatementType == StatementType.DropIndex)
            {
                RecordManager.DropIndex(listener.DatabaseName, listener.TableName, listener.IndexName, _client, _catalogManager);
            }
            else if (listener.StatementType == StatementType.Insert)
            {
                Dictionary<string, object> rowData = new Dictionary<string, object>();
                //string? primaryKeyColumn = null;

                //logger.LogMessage("Before calling Insert method...");
                //logger.LogMessage("Columns: " + string.Join(", ", listener.Columns.Keys));
                //logger.LogMessage("Values: " + string.Join(", ", listener.Columns.Values));

                //primaryKeyColumn = _catalogManager?.GetPrimaryKeyColumn(listener.DatabaseName, listener.TableName)
                //        ?? throw new Exception("ERROR: Primary key not found!");

                // Iterate through the listener.Columns dictionary
                foreach (var column in listener.Columns)
                {
                    rowData[column.Key] = column.Value;
                }

                //logger.LogMessage("Row data: " + string.Join(", ", rowData.Select(kv => $"{kv.Key}={kv.Value}")));

                RecordManager.Insert(listener.DatabaseName, listener.TableName, rowData, _client, _catalogManager);

                //logger.LogMessage("After calling Insert method...");

                //Query(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
                //PrintAllDocuments(listener.DatabaseName, listener.TableName);
                var conditions = listener.Conditions;
                RecordManager.Delete(listener.DatabaseName, listener.TableName,conditions, _client, _catalogManager);
                //PrintAllDocuments(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Select)
            {
                if (listener.JoinConditions.Count > 0)
                {
                    var tableNames = new List<string>
                    {
                        listener.TableName
                    };
                    foreach (var condition in listener.JoinConditions)
                    {
                        tableNames.Add(condition.TableAlias);
                    }
                    HandleSelectStatementWithJoin(listener.DatabaseName, tableNames, listener.JoinConditions,listener.Conditions ,listener.SelectedColumns);
                }
                else
                {
                    var tableNames = new List<string>
                    {
                        listener.TableName
                    };
                    HandleSelectStatement(listener.DatabaseName, tableNames, listener.Conditions, listener.SelectedColumns);
                }
            }
            else
            {
                throw new Exception("ERROR: Unknown command type!");
            }


            return Task.CompletedTask;
        }

        public void ListDatabases()
        {
            var databaseList = _client?.ListDatabaseNames().ToList()
                ?? throw new Exception("No Databases present.");
            logger.LogMessage("Databases:");
            foreach (var databaseName in databaseList)
            {
                logger.LogMessage(databaseName);
            }
        }

        public static void Query(string databaseName, string tableName)
        {
            logger.LogMessage($"Querying {databaseName}.{tableName}");
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName)
                ?? throw new Exception("ERROR: Table not found! Null ref");

            var filter = Builders<BsonDocument>.Filter.Empty;
            var documents = collection.Find(filter).ToList();

            logger.LogMessage("Results:");
            foreach (var document in documents)
            {
                logger.LogMessage(document.ToString());
            }
        }

        public static void PrintAllDocuments(string databaseName, string tableName)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(Builders<BsonDocument>.Filter.Empty).ToList();

            logger.LogMessage($"Documents in {databaseName}.{tableName}:");
            foreach (var document in documents)
            {
                logger.LogMessage(document.ToString());
            }
        }


        private static void HandleSelectStatement(string databaseName, List<string >tableName, List<FilterCondition> conditions, string[] selectedColumns)
        {
            ValidateDatabaseAndTable(databaseName, tableName);
            var tName = tableName[0];
            var rows = GetRows(databaseName, tName, conditions);
            logger.LogMessage($"Rows: {rows.Count}");
            var formattedResult = FormatOutput(rows, selectedColumns, databaseName, tName);
            LastQueryResult = formattedResult;
        }

        private static void HandleSelectStatementWithJoin(string databaseName, List<string> tableNames, List<JoinCondition> joinConditions, List<FilterCondition> conditions, string[] selectedColumns)
        {
            //if (tableNames.Count < 2 || joinConditions.Count != tableNames.Count - 1)
            //    throw new Exception("Incorrect number of tables or join conditions provided");

            //ValidateDatabaseAndTable(databaseName, tableNames);

            //// Assume the first table is the outer table
            //var rows = GetRows(databaseName, tableNames[0], conditions);

            //for (int i = 1; i < tableNames.Count; i++)
            //{
            //    var joinRows = GetIndexedRows(databaseName, tableNames[i], joinConditions[i - 1].Column2);

            //    rows = IndexedNestedLoopJoin(rows, joinRows, joinConditions[i - 1].Column1, joinConditions[i - 1].Column2, tableNames[i]);
            //}

            //var formattedResult = FormatOutput(rows, selectedColumns, databaseName, tableNames[0]);
            //LastQueryResult = formattedResult;
        }

        //private static Dictionary<object,Dictionary<string, object>> GetIndexedRows()
        //{

        //}


        private static List<Dictionary<string, object>> IndexedNestedLoopJoin(
    List<Dictionary<string, object>> outerRows,
    Dictionary<object, Dictionary<string, object>> indexedInnerRows,
    string outerColumn,
    string innerColumn,
    string innerTableName)
        {
            var mergedRows = new List<Dictionary<string, object>>();
            foreach (var outerRow in outerRows)
            {
                if (indexedInnerRows.ContainsKey(outerRow[outerColumn]))
                {
                    var innerRow = indexedInnerRows[outerRow[outerColumn]];
                    var mergedRow = new Dictionary<string, object>(outerRow);

                    foreach (var column in innerRow)
                    {
                        mergedRow[$"{innerTableName}.{column.Key}"] = column.Value;
                    }

                    mergedRows.Add(mergedRow);
                }
            }
            return mergedRows;
        }


        private static void ValidateDatabaseAndTable(string databasename, List<string> tableNames)
        {
            if (tableNames.Any(string.IsNullOrEmpty) || databasename == null)
            {
                throw new Exception("Database or table name missing in statement");
            }
        }

        private static List<Dictionary<string, object>> MergeRows(List<Dictionary<string, object>> rows, List<Dictionary<string, object>> joinRows, string conditionColumnName, string joinedTableName)
        {
            var mergedRows = new List<Dictionary<string, object>>();
            foreach (var row in rows)
            {
                foreach (var joinRow in joinRows)
                {
                    if (row[conditionColumnName].Equals(joinRow[conditionColumnName]))
                    {
                        var mergedRow = new Dictionary<string, object>(row);
                        foreach (var column in joinRow)
                        {
                            mergedRow[$"{joinedTableName}.{column.Key}"] = column.Value;
                        }
                        mergedRows.Add(mergedRow);
                    }
                }
            }
            return mergedRows;
        }

        private static List<Dictionary<string, object>> GetRows(string databaseName, string tableName, List<FilterCondition> conditions)
        {
            return RecordManager.GetRowsSatisfyingConditions(databaseName, tableName, conditions, _client, _catalogManager);
        }

        private static string FormatOutput(List<Dictionary<string, object>> rows, string[] selectedColumns, string databaseName, string tableName)
        {
            StringBuilder resultStringBuilder = new StringBuilder();
            int columnWidth = 20; // Define the width of your columns, can be adjusted based on needs

            if (selectedColumns.Length > 0 && !selectedColumns.Contains("*"))
            {
                var line = "| " + string.Join(" | ", selectedColumns.Select(column => column.PadRight(columnWidth))) + " |";
                resultStringBuilder.AppendLine("+" + new string('-', line.Length - 2) + "+");
                resultStringBuilder.AppendLine(line);

                foreach (var row in rows)
                {
                    resultStringBuilder.AppendLine("| " + string.Join(" | ", selectedColumns.Select(column => row[column]?.ToString().PadRight(columnWidth))) + " |");
                }
            }
            else
            {
                var columns = rows[0].Keys.ToList();
                var line = "| " + string.Join(" | ", columns.Select(column => column.PadRight(columnWidth))) + " |";
                resultStringBuilder.AppendLine("+" + new string('-', line.Length - 2) + "+");
                resultStringBuilder.AppendLine(line);

                foreach (var row in rows)
                {
                    resultStringBuilder.AppendLine("| " + string.Join(" | ", columns.Select(column => row[column]?.ToString().PadRight(columnWidth))) + " |");
                }
            }

            resultStringBuilder.AppendLine("+" + new string('-', resultStringBuilder.ToString().Split('\n')[0].Length - 2) + "+");
            return resultStringBuilder.ToString();
        }


    }
}
