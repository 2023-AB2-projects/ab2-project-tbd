using MongoDB.Driver;
using MongoDB.Bson;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using System.Xml.Linq;
using System.Diagnostics.CodeAnalysis;
using abkrServer.CatalogManager.RecordManager;
using abkr.ServerLogger;
using System.Text;
using Newtonsoft.Json;
using abkrServer.Parser.Listener;
using System.Collections.ObjectModel;

namespace abkr.CatalogManager
{
    public class DatabaseData
    {
        public string? Name { get; set; }
        public ObservableCollection<string>? Tables { get; set; }
    }
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

            var listener = new MyAbkrGrammarListener("C:/Users/Simon Zoltán/Desktop/ab2-project-tbd/abkrServer/Parser/example.xml", _catalogManager, logger);
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
                RecordManager.CreateIndex(listener.DatabaseName, listener.TableName, listener.IndexName, listener.IndexColumns, _catalogManager.IsUniqueKey(listener.DatabaseName, listener.TableName, listener.ColumnName), _client, _catalogManager);
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

               // logger.LogMessage("After calling Insert method...");

                Query(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
               // PrintAllDocuments(listener.DatabaseName, listener.TableName);
                var conditions = listener.Conditions;
                RecordManager.Delete(listener.DatabaseName, listener.TableName,conditions, _client, _catalogManager);
                //PrintAllDocuments(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Select)
            {
                if (listener.JoinConditions.Count > 0)
                {
                    foreach (var joinCondition in listener.JoinConditions)
                    {
                        HandleSelectStatementWithJoin(listener.DatabaseName, listener.TableName, joinCondition.TableAlias, joinCondition, listener.Conditions, listener.SelectedColumns);
                    }
                }
                else
                {
                    HandleSelectStatement(listener.DatabaseName, listener.TableName, listener.Conditions, listener.SelectedColumns);
                }
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

        public List<DatabaseData> GetDatabasesAndTables()
        {
            var databaseList = _client?.ListDatabaseNames().ToList()
             ?? throw new Exception("No Databases present.");

            var databases = new List<DatabaseData>();

            foreach (var databaseName in databaseList)
            {
                var database = _client.GetDatabase(databaseName);
                var tables = new ObservableCollection<string>(database.ListCollectionNames().ToList());
                databases.Add(new DatabaseData { Name = databaseName, Tables = tables });
            }

            return databases;
        }

        // Refactored function
        private static void HandleSelectStatement(string databaseName, string tableName, List<FilterCondition> conditions, string[] selectedColumns)
        {
            ValidateDatabaseAndTable(databaseName, tableName);
            // Retrieve rows from the collection that satisfy the conditions
            var rows = GetRows(databaseName, tableName, conditions);

            LastQueryResult = JsonConvert.SerializeObject(rows, Formatting.Indented);

            //logger.LogMessage(LastQueryResult);
        }

        private static void HandleSelectStatementWithJoin(string databaseName, string tableName, string joinedTableName, JoinCondition joinCondition, List<FilterCondition> conditions, string[] selectedColumns)
        {
            ValidateDatabaseAndTable(databaseName, tableName, joinedTableName);
            var rows = GetRows(databaseName, tableName, conditions);
            var joinConditions = new List<FilterCondition>
            {
                new FilterCondition(joinCondition.ConditionColumnName, "=", joinCondition.ConditionValue)
            };
            var joinRows = GetRows(databaseName, joinedTableName, joinConditions);
            var mergedRows = MergeRows(rows, joinRows, joinCondition.ConditionColumnName, joinedTableName);
            
            LastQueryResult = JsonConvert.SerializeObject(rows, Formatting.Indented);

        }
        private static void ValidateDatabaseAndTable(params string[] names)
        {
            if (names.Any(string.IsNullOrEmpty))
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
            // Additional helper functions
            private static IMongoDatabase GetDatabase(string databaseName)
        {
            return _client?.GetDatabase(databaseName);
        }

        private static IMongoCollection<BsonDocument> GetCollection(IMongoDatabase database, string tableName)
        {
            return database?.GetCollection<BsonDocument>(tableName);
        }

        private static List<BsonDocument> GetDocuments(string databaseName, string tableName, List<FilterCondition> conditions)
        {
            return RecordManager.GetDocumentsSatisfyingConditions(databaseName, tableName, conditions, _client, _catalogManager);
        }

        public static List<Dictionary<string, object>> GetRows(string databaseName, string tableName, List<FilterCondition> conditions)
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


