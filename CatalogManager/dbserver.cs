using MongoDB.Driver;
using MongoDB.Bson;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using abkr.grammarParser;


namespace abkr.CatalogManager
{

    public class DatabaseServer
    {
        private IMongoClient _client;
        private CatalogManager _catalogManager;


        public DatabaseServer(string connectionString, string metadataFilePath)
        {
            _client = new MongoClient(connectionString);
            _catalogManager = new CatalogManager(metadataFilePath);
        }

        public bool IsMetadataInSync()
        {
            var metadata = _catalogManager.LoadMetadata(); // This method should be made public in CatalogManager
            var databaseList = _client.ListDatabaseNames().ToList();

            foreach (var databaseName in metadata.Keys)
            {
                if (!databaseList.Contains(databaseName))
                {
                    return false;
                }

                var collectionList = _client.GetDatabase(databaseName).ListCollectionNames().ToList();
                var tableMetadata = metadata[databaseName] as Dictionary<string, object>;

                foreach (var tableName in tableMetadata.Keys)
                {
                    if (!collectionList.Contains(tableName))
                    {
                        return false;
                    }

                    var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
                    var indexList = collection.Indexes.List().ToList();
                    var indexMetadata = ((tableMetadata[tableName] as Dictionary<string, object>)["indexes"]) as Dictionary<string, object>;

                    foreach (var indexName in indexMetadata.Keys)
                    {
                        if (!indexList.Any(index => index["name"] == indexName))
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }


        public void CreateDatabase(string databaseName)
        {
            _catalogManager.CreateDatabase(databaseName);
        }

        public void CreateTable(string databaseName, string tableName, Dictionary<string, object> columns)
        {
            var bsonColumns = BsonDocument.Parse(JsonConvert.SerializeObject(columns));
            _catalogManager.CreateTable(databaseName, tableName, columns);
        }

        public void DropDatabase(string databaseName)
        {
            _client.DropDatabase(databaseName);
            _catalogManager.DropDatabase(databaseName);
        }

        public void DropTable(string databaseName, string tableName)
        {
            var database = _client.GetDatabase(databaseName);
            database.DropCollection(tableName);
            _catalogManager.DropTable(databaseName, tableName);
        }

        public void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var indexKeys = new IndexKeysDefinitionBuilder<BsonDocument>().Ascending((FieldDefinition<BsonDocument>)columns.Select(column => column.AsString));
            collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(indexKeys, new CreateIndexOptions { Name = indexName }));

            var metadata = _catalogManager.LoadMetadata(); // This method should be made public in CatalogManager
            var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
            var tableMetadata = databaseMetadata[tableName] as Dictionary<string, object>;
            var indexes = tableMetadata["indexes"] as Dictionary<string, object>;

            _catalogManager.CreateIndex(databaseName, tableName, indexName, columns, indexes);
        }
        public void DropIndex(string databaseName, string tableName, string indexName)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            collection.Indexes.DropOne(indexName);

            _catalogManager.DropIndex(databaseName, tableName, indexName);
        }

        public void Insert(string databaseName, string tableName, Dictionary<string, object> rowData)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var document = new BsonDocument(rowData.ToDictionary(kvp => kvp.Key, kvp => BsonValue.Create(kvp.Value)));
            collection.InsertOne(document);
        }

        public void Delete(string databaseName, string tableName, string primaryKeyColumn, object primaryKeyValue)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var filter = Builders<BsonDocument>.Filter.Eq(primaryKeyColumn, BsonValue.Create(primaryKeyValue));
            collection.DeleteOne(filter);
        }

        public void ExecuteStatement(string sql)
        {
            // Create a new instance of the ANTLR input stream with the SQL statement
            Console.WriteLine(sql);
            var inputStream = new AntlrInputStream(sql);

            // Create a new instance of the lexer and pass the input stream
            var lexer = new abkr_grammarLexer(inputStream);

            // Create a new instance of the common token stream and pass the lexer
            var tokenStream = new CommonTokenStream(lexer);

            // Create a new instance of the parser and pass the token stream
            var parser = new abkr_grammarParser(tokenStream);

            // Invoke the parser's entry rule (statement) and get the parse tree
            var parseTree = parser.statement();

            // Implement your own parse tree listener (MyAbkrGrammarListener) to process the parse tree and extract the required information
            var listener = new MyAbkrGrammarListener();
            ParseTreeWalker.Default.Walk(listener, parseTree);

            // Perform actions based on the parsed statement
            if (listener.StatementType == StatementType.CreateDatabase)
            {
                CreateDatabase(listener.DatabaseName);
            }
            else if (listener.StatementType == StatementType.CreateTable)
            {
                CreateTable(listener.DatabaseName, listener.TableName, listener.Columns);
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
                Insert(listener.DatabaseName, listener.TableName, listener.RowData);
            }
            else if (listener.StatementType == StatementType.Delete)
            {
                Delete(listener.DatabaseName, listener.TableName, listener.PrimaryKeyColumn, listener.PrimaryKeyValue);
            }
        }
    }
  }

