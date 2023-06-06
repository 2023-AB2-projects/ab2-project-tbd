using MongoDB.Driver;
using MongoDB.Bson;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using System.Xml.Linq;
using System.Diagnostics.CodeAnalysis;
using abkrServer.CatalogManager.RecordManager;
using abkr.ServerLogger;
using System.Text;

namespace abkr.CatalogManager
{

    public class DatabaseServer
    {
        static private IMongoClient _client;
        static public IMongoClient MongoClient => _client;
        static private CatalogManager _catalogManager;
        static private Logger logger;

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
            var parser = new abkr_grammar(tokenStream);

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
                    var foreignKeyReference = listener.ForeignKeyColumns.Where(fk => fk.ColumnName == columnName).FirstOrDefault();
                    if (foreignKeyReference != null)
                    {
                        logger.LogMessage($"DatabaseServer.ExecuteStatement: Column {columnName} is a foreign key reference to {foreignKeyReference}");
                    }


                    if (isPrimaryKey)
                    {
                        isUnique = true;
                    }

                    var newColumn = new Column(columnName, columnType, isUnique, isPrimaryKey, foreignKeyReference);

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

                logger.LogMessage("Before calling Insert method...");
                logger.LogMessage("Columns: " + string.Join(", ", listener.Columns.Keys));
                logger.LogMessage("Values: " + string.Join(", ", listener.Columns.Values));

                //primaryKeyColumn = _catalogManager?.GetPrimaryKeyColumn(listener.DatabaseName, listener.TableName)
                //        ?? throw new Exception("ERROR: Primary key not found!");

                // Iterate through the listener.Columns dictionary
                foreach (var column in listener.Columns)
                {
                    rowData[column.Key] = column.Value;
                }

                logger.LogMessage("Row data: " + string.Join(", ", rowData.Select(kv => $"{kv.Key}={kv.Value}")));

                RecordManager.Insert(listener.DatabaseName, listener.TableName, rowData, _client, _catalogManager);

                logger.LogMessage("After calling Insert method...");

                Query(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
                var conditions = listener.Conditions;
                RecordManager.Delete(listener.DatabaseName, listener.TableName,conditions, _client, _catalogManager);
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Select)
            {

                HandleSelectStatement(listener.DatabaseName, listener.TableName, listener.Conditions, listener.SelectedColumns);
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


        private static void HandleSelectStatement(string databaseName, string tableName, List<FilterCondition> conditions, string[] selectedColumns)
        {
            if (string.IsNullOrEmpty(databaseName) || string.IsNullOrEmpty(tableName))
            {
                throw new Exception("Database or table name missing in SELECT statement");
            }

            var _database = _client?.GetDatabase(databaseName);
            var _collection = _database?.GetCollection<BsonDocument>(tableName);

            // Step 1: Retrieve documents from the collection that satisfy the conditions
            var documents = RecordManager.GetDocumentsSatisfyingConditions(databaseName, tableName, conditions, _client, _catalogManager);

            // If there are specific columns selected
            if (selectedColumns.Length > 0 && !selectedColumns.Contains("*"))
            {
                foreach (BsonDocument document in documents)
                {
                    var row = RecordManager.ConvertDocumentToRow(document, _catalogManager, databaseName, tableName);
                    foreach (string column in selectedColumns)
                    {
                        logger.LogMessage($"{column}: {row[column]}");
                    }
                }
            }
            // If all columns are selected (with asterisk '*')
            else
            {
                foreach (BsonDocument document in documents)
                {
                    var row = RecordManager.ConvertDocumentToRow(document, _catalogManager, databaseName, tableName);
                    var stringBuilder = new StringBuilder();
                    stringBuilder.AppendLine("Record:");
                    foreach (var item in row)
                    {
                        stringBuilder.AppendLine($"{item.Key}: {item.Value}");
                    }
                    logger.LogMessage(stringBuilder.ToString());
                }
            }
        }


    }
}


