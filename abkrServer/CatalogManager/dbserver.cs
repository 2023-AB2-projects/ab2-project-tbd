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

        public void ListDatabases()
        {
            var databaseList = _client?.ListDatabaseNames().ToList()
                ??throw new Exception("No Databases present.");
            Console.WriteLine("Databases:");
            foreach (var databaseName in databaseList)
            {
                Console.WriteLine(databaseName);
            }
        }


        public static void CreateDatabase(string databaseName)
        {
            _client?.GetDatabase(databaseName);   
            Console.WriteLine($"Creating database: {databaseName}");
            _catalogManager?.CreateDatabase(databaseName);
        }

        public static void CreateTable(string databaseName, string tableName, List<Column> columns)
        {

            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be null or empty.");
            }

            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(nameof(tableName), "Table name cannot be null or empty.");
            }

            if (columns == null || columns.Count == 0)
            {
                throw new ArgumentNullException(nameof(columns), "Columns cannot be null or empty.");
            }

            var database = (_client?.GetDatabase(databaseName)) 
                ?? throw new Exception("Database connection not established.");

            
            database?.CreateCollection(tableName);
            Console.WriteLine($"Creating table: {databaseName}.{tableName}");

            var columnsDictionary = columns?.ToDictionary(column => column.Name, column => column.Type);
            var primaryKeyColumn = columns.FirstOrDefault(column => column.IsPrimaryKey)?.Name;
            var uniqueKeys = columns.Where(column => column.IsUnique).Select(column => column.Name).ToList();
            var foreignKeys = columns
                .Where(column => !string.IsNullOrEmpty(column.ForeignKeyReference))
                .ToDictionary(column => column.Name, column => column.ForeignKeyReference);

            _catalogManager?.CreateTable(databaseName, tableName, columnsDictionary, primaryKeyColumn, foreignKeys, uniqueKeys);

            // Create index files for unique keys
            foreach (var uniqueKey in uniqueKeys)
            {
                CreateIndex(databaseName, tableName, $"{uniqueKey}_unique", new BsonArray(new[] { uniqueKey }), true);
            }

            // If there's a foreign key column, create an index for it.
            foreach (var foreignKey in foreignKeys)
            {
                CreateIndex(databaseName, tableName, $"{foreignKey.Key}_fk", new BsonArray(new[] { foreignKey.Key }), false);
            }
        }


        public static void CreateNonUniqueIndex(string databaseName, string tableName, string indexColumnName, string primaryKeyName)
        {
            var database = _client?.GetDatabase(databaseName);
            var collection = database?.GetCollection<BsonDocument>(tableName);

            var nonUniqueIndexCollection = database?.GetCollection<BsonDocument>(tableName + "_" + indexColumnName + "_NonUniqueIndex");
            nonUniqueIndexCollection?.DeleteMany(new BsonDocument()); // Clear the non-unique index collection if it already exists

            var nonUniqueIndex = new Dictionary<string, List<string>>();

            var cursor = collection.Find(new BsonDocument()).ToCursor();
            foreach (var document in cursor.ToEnumerable())
            {
                var indexColumnValue = document[indexColumnName].AsString;
                var primaryKeyValue = document[primaryKeyName].AsString;

                if (!nonUniqueIndex.ContainsKey(indexColumnValue))
                {
                    nonUniqueIndex[indexColumnValue] = new List<string>();
                }

                nonUniqueIndex[indexColumnValue].Add(primaryKeyValue);
            }

            foreach (var pair in nonUniqueIndex)
            {
                var indexDocument = new BsonDocument
        {
            { "IndexColumnValue", pair.Key },
            { "PrimaryKeys", new BsonArray(pair.Value) }
        };
                nonUniqueIndexCollection?.InsertOne(indexDocument);
            }
        }

        public static void DropDatabase(string databaseName)
        {
            Console.WriteLine($"Dropping database: {databaseName}");
            _client?.DropDatabase(databaseName);
            _catalogManager?.DropDatabase(databaseName);
        }

        public static void DropTable(string databaseName, string tableName)
        {
            Console.WriteLine($"Dropping table: {databaseName}.{tableName}");
            var database = _client.GetDatabase(databaseName);
            database.DropCollection(tableName);
            _catalogManager?.DropTable(databaseName, tableName);
        }

        public static void Insert(string databaseName, string tableName, string primaryKeyColumn, List<Dictionary<string, object>> rowsData)
        {
            Console.WriteLine($"Inserting rows into {databaseName}.{tableName}");
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName)
                ?? throw new Exception("ERROR: Table not found! Null ref");

            List<BsonDocument> documents = new();

            try
            {
                foreach (var rowData in rowsData)
                {
                    Console.WriteLine("Row data:");
                    var primaryKeyValue = rowData[primaryKeyColumn];
                    Console.WriteLine(primaryKeyColumn + " = " + primaryKeyValue);
                    rowData.Remove(primaryKeyColumn);

                    var document = new BsonDocument
            {
                { "_id", BsonValue.Create(primaryKeyValue) },
                { "value", new BsonDocument(rowData.ToDictionary(kvp => kvp.Key, kvp => BsonValue.Create(kvp.Value))) }
            };

                    documents.Add(document);
                }
                Console.WriteLine("Row data:");
                foreach (var doc in documents)
                {
                    Console.WriteLine(doc.ToString());
                }
                collection.InsertMany(documents);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error during Insert operation: " + ex.Message);
            }
        }




        public static void Delete(string databaseName, string tableName, FilterDefinition<BsonDocument> filter)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var deleteResult = collection.DeleteMany(filter);
            Console.WriteLine($"Deleted count: {deleteResult.DeletedCount}");
        }

        public static void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns, bool isUnique)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            var indexKeysBuilder = new IndexKeysDefinitionBuilder<BsonDocument>();
            var indexKeysDefinitions = new List<IndexKeysDefinition<BsonDocument>>();

            foreach (var column in columns)
            {
                indexKeysDefinitions.Add(indexKeysBuilder.Ascending(column.AsString));
            }

            var indexKeys = Builders<BsonDocument>.IndexKeys.Combine(indexKeysDefinitions);

            var indexOptions = new CreateIndexOptions { Name = indexName, Unique = isUnique };
            collection?.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(indexKeys, indexOptions));

            _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns.Select(column => column.AsString).ToList(), isUnique);
        }


        public static void DropIndex(string databaseName, string tableName, string indexName)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            collection?.Indexes.DropOne(indexName);

            _catalogManager?.DropIndex(databaseName, tableName, indexName);
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
                CreateDatabase(listener.DatabaseName);
            }
            else if (listener.StatementType == StatementType.CreateTable)
            {
                var columns = new List<Column>();
                foreach (var column in listener.Columns)
                {
                    // Assuming Column has properties Name and Value
                    columns.Add(new Column() { Name = column.Key, Type = column.Value });
                }
                CreateTable(listener.DatabaseName, listener.TableName, columns);
            }

            else if (listener.StatementType == StatementType.DropDatabase)
            {
                DropDatabase(listener.DatabaseName);
            }
            else if (listener.StatementType == StatementType.DropTable)
            {
                DropTable(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.CreateIndex)
            {
                CreateIndex(listener.DatabaseName, listener.TableName, listener.IndexName, listener.IndexColumns, _catalogManager.IsUniqueKey(listener.DatabaseName,listener.TableName,listener.ColumnName));
            }
            else if (listener.StatementType == StatementType.DropIndex)
            {
                DropIndex(listener.DatabaseName, listener.TableName, listener.IndexName);
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

                Insert(listener.DatabaseName, listener.TableName, primaryKeyColumn, new List<Dictionary<string, object>> { rowData });

                Console.WriteLine("After calling Insert method...");

                Query(listener.DatabaseName, listener.TableName);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
                Console.WriteLine($"Deleting row from {listener.DatabaseName}.{listener.TableName}");
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
                Delete(listener.DatabaseName, listener.TableName, listener.DeleteFilter);
                PrintAllDocuments(listener.DatabaseName, listener.TableName);
            }
            else if(listener.StatementType == StatementType.Select)
            {

                HandleSelectStatement(listener.DatabaseName,listener.TableName,listener.SelectFilter,listener.SelectedColumns);
            }

            return Task.CompletedTask;
        }
    }
}

