using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections.Generic;
using System.IO;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using abkr.grammarParser;
using System.Xml.Linq;
using Amazon.Auth.AccessControlPolicy;


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

        public bool IsMetadataInSync()
        {
            return IsMetadataInSync(_catalogManager);
        }

        public static bool IsMetadataInSync(CatalogManager? _catalogManager)
        {
            XElement? metadata = _catalogManager?.LoadMetadata()
                ??throw new Exception("ERROR: Metadata does not exist!");

            Console.Write(metadata?.ToString());
            var databaseList = _client?.ListDatabaseNames().ToList()
                ??throw new Exception("ERROR: No databases!");

            foreach (var databaseElement in metadata.Elements("database"))
            {
                string? databaseName = databaseElement?.Attribute("name")?.Value;

                if (!databaseList.Contains(databaseName))
                {
                    return false;
                }

                var collectionList = _client?.GetDatabase(databaseName).ListCollectionNames().ToList()
                    ??throw new Exception("ERROR: Tables not found!");

                foreach (var tableElement in databaseElement.Elements("table"))
                {
                    string? tableName = tableElement?.Attribute("name")?.Value
                        ??throw new Exception("ERROR: Table name not found!");

                    if (!collectionList.Contains(tableName))
                    {
                        return false;
                    }

                    var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
                    var indexList = collection.Indexes.List().ToList();

                    foreach (var indexElement in tableElement.Elements("index"))
                    {
                        string? indexName = indexElement?.Attribute("name")?.Value;

                        if (!indexList.Any(index => index["name"] == indexName))
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }





        public static void CreateDatabase(string databaseName)
        {
            _client?.GetDatabase(databaseName);   
            Console.WriteLine($"Creating database: {databaseName}");
            _catalogManager?.CreateDatabase(databaseName);
        }

        public static void CreateTable(string databaseName, string tableName, Dictionary<string, string> columns, string primaryKeyColumn, List<string> uniqueKeys = null, Dictionary<string, string> foreignKeys = null)
        {
            var database = _client?.GetDatabase(databaseName);
            database?.CreateCollection(tableName);
            Console.WriteLine($"Creating table: {databaseName}.{tableName}");
            _catalogManager?.CreateTable(databaseName, tableName, columns, primaryKeyColumn);

            // Create index files for unique keys
            if (uniqueKeys != null)
            {
                foreach (var uniqueKey in uniqueKeys)
                {
                    CreateIndex(databaseName, tableName, $"{uniqueKey}_unique", new BsonArray(new[] { uniqueKey }));
                }
            }

            // If there's a foreign key column, create an index for it.
            if (foreignKeys != null)
            {
                foreach (var foreignKey in foreignKeys)
                {
                    CreateIndex(databaseName, tableName, $"{foreignKey.Key}_fk", new BsonArray(new[] { foreignKey.Key }));
                }
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




        public static void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var indexKeys = new IndexKeysDefinitionBuilder<BsonDocument>().Ascending((FieldDefinition<BsonDocument>)columns.Select(column => column.AsString));
            collection?.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(indexKeys, new CreateIndexOptions { Name = indexName }));

            XElement? metadata = _catalogManager?.LoadMetadata(); 
            XElement? databaseMetadata = metadata?.Element(databaseName);
            XElement? tableMetadata = databaseMetadata?.Element(tableName);
            XElement? indexes = tableMetadata?.Element("indexes");

            _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns.Select(column => column.AsString).ToList(), false); // Convert BsonArray to List<string>
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
            var parser = new abkr_grammarParser(tokenStream);

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
                var stringColumns = new Dictionary<string, string>();
                foreach (var column in listener.Columns)
                {
                    stringColumns[column.Key] = column.Value.ToString();
                }
                CreateTable(listener.DatabaseName, listener.TableName, stringColumns, listener.PrimaryKeyColumn);
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
                CreateIndex(listener.DatabaseName, listener.TableName, listener.IndexName, listener.IndexColumns);
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

            return Task.CompletedTask;
        }
    }
}

