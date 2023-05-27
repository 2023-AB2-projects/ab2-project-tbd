using MongoDB.Driver;
using MongoDB.Bson;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using System.Xml.Linq;
using System.Diagnostics.CodeAnalysis;

namespace abkr.CatalogManager
{

    public class DatabaseServer
    {
        static private IMongoClient? _client;
        static public IMongoClient? MongoClient => _client;
        
        static private CatalogManager? _catalogManager;


        public DatabaseServer(string connectionString, string metadataFileName)
        {
            _client = new MongoClient(connectionString);
            string metadataFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, metadataFileName);

            if (!File.Exists(metadataFilePath))
            {
                XElement emptyMetadata = new XElement("metadata");
                File.WriteAllText(metadataFilePath, emptyMetadata.ToString()+"\n");
            }

            _catalogManager = new CatalogManager(metadataFilePath);
        }

        public static Task ExecuteStatementAsync(string sql)
        {
            Console.WriteLine("ExecuteStatementAsync called");

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

            var listener = new MyAbkrGrammarListener("C:/Users/bfcsa/source/repos/abkr/abkrServer/Parser/example.xml");
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
                    // Assuming Column has properties Name and Value
                    Console.WriteLine("Column: " + column.Key + " " + column.Value);
                    columns.Add(new Column() { Name = column.Key, Type = column.Value });
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
                string? primaryKeyColumn = null;

                Console.WriteLine("Before calling Insert method...");
                Console.WriteLine("Columns: " + string.Join(", ", listener.Columns.Keys));
                Console.WriteLine("Values: " + string.Join(", ", listener.Columns.Values));

                primaryKeyColumn = _catalogManager?.GetPrimaryKeyColumn(listener.DatabaseName, listener.TableName)
                        ?? throw new Exception("ERROR: Primary key not found!");

                // Iterate through the listener.Columns dictionary
                foreach (var column in listener.Columns)
                {
                    rowData[column.Key] = column.Value;
                }

                Console.WriteLine("Row data: " + string.Join(", ", rowData.Select(kv => $"{kv.Key}={kv.Value}")));

                RecordManager.Insert(listener.DatabaseName, listener.TableName, primaryKeyColumn, new List<Dictionary<string, object>> { rowData }, _client, _catalogManager);

                Console.WriteLine("After calling Insert method...");

                Query(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
                Console.WriteLine($"Deleting row from {listener.DatabaseName}.{listener.TableName}");
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
                RecordManager.Delete(listener.DatabaseName, listener.TableName, listener.DeleteFilter, _client, _catalogManager);
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Select)
            {

                HandleSelectStatement(listener.DatabaseName, listener.TableName, listener.SelectFilter, listener.SelectedColumns);
            }

            return Task.CompletedTask;
        }

        public void ListDatabases()
        {
            var databaseList = _client?.ListDatabaseNames().ToList()
                ?? throw new Exception("No Databases present.");
            Console.WriteLine("Databases:");
            foreach (var databaseName in databaseList)
            {
                Console.WriteLine(databaseName);
            }
        }

        public static void Query(string databaseName, string tableName)
        {
            Console.WriteLine($"Querying {databaseName}.{tableName}");
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName)
                ?? throw new Exception("ERROR: Table not found! Null ref");

            var filter = Builders<BsonDocument>.Filter.Empty;
            var documents = collection.Find(filter).ToList();

            Console.WriteLine("Results:");
            foreach (var document in documents)
            {
                Console.WriteLine(document.ToString());
            }
        }

        public static void PrintAllDocuments(string databaseName, string tableName)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(Builders<BsonDocument>.Filter.Empty).ToList();

            Console.WriteLine($"Documents in {databaseName}.{tableName}:");
            foreach (var document in documents)
            {
                Console.WriteLine(document);
            }
        }


        private static void HandleSelectStatement(string databaseName, string tableName, FilterDefinition<BsonDocument> filter, string[] selectedColumns)
        {
            if (string.IsNullOrEmpty(databaseName) || string.IsNullOrEmpty(tableName))
            {
                throw new Exception("Database or table name missing in SELECT statement");
            }

            var _database = _client?.GetDatabase(databaseName);
            var _collection = _database?.GetCollection<BsonDocument>(tableName);

            List<BsonDocument> documents;

            if (filter != null)
            {
                documents = _collection.Find(filter).ToList();
            }
            else
            {
                documents = _collection.Find(new BsonDocument()).ToList();
            }

            foreach (BsonDocument document in documents)
            {
                // If specific columns are selected
                if (selectedColumns.Length > 0)
                {
                    foreach (string column in selectedColumns)
                    {
                        Console.WriteLine($"{column}: {document[column]}");
                    }
                }
                // If all columns are selected
                else
                {
                    foreach (BsonElement element in document)
                    {
                        Console.WriteLine($"{element.Name}: {element.Value}");
                    }
                }
            }
        }
    }
}

