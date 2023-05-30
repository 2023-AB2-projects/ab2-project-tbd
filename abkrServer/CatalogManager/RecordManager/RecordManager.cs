using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace abkr.CatalogManager
{
    internal class RecordManager
    {


        public static void CreateDatabase(string databaseName, IMongoClient _client, CatalogManager _catalogManager)
        {
            _client?.GetDatabase(databaseName);
            Console.WriteLine($"Creating database: {databaseName}");
            _catalogManager?.CreateDatabase(databaseName);
        }

        public static void CreateTable(string databaseName, string tableName, List<Column> columns, CatalogManager _catalogManager, IMongoClient _client)
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

            //var columnsDictionary = columns?.ToDictionary(column => column.Name, column => column.Type);
            var primaryKeyColumn = columns.FirstOrDefault(column => column.IsPrimaryKey)?.Name
                ??throw new Exception("RecordManager.CreateTable: Primary key not found.");
            var uniqueKeys = columns.Where(column => column.IsUnique).Select(column => column.Name).ToList();
            var foreignKeys = columns?.Where(column => !string.IsNullOrEmpty(column.ForeignKeyReference))?.ToDictionary(column => column.Name, column => column.ForeignKeyReference);

            _catalogManager?.CreateTable(databaseName, tableName, columns, primaryKeyColumn, foreignKeys, uniqueKeys);

            // Create index files for unique keys
            foreach (var uniqueKey in uniqueKeys)
            {
                Console.WriteLine($"Creating unique key:{uniqueKey}");
                CreateIndex(databaseName, tableName, $"{uniqueKey}_unique", new BsonArray(new[] { uniqueKey }), true, _client, _catalogManager);
            }

            // If there's a foreign key column, create an index for it.
            foreach (var foreignKey in foreignKeys)
            {
                Console.WriteLine($"Creating foreign key:{foreignKey.Key}");
                CreateIndex(databaseName, tableName, $"{foreignKey.Key}_fk", new BsonArray(new[] { foreignKey.Key }), false, _client, _catalogManager);
            }
        }

        public static void CreateNonUniqueIndex(string databaseName, string tableName, string indexColumnName, string primaryKeyName, IMongoClient _client, CatalogManager _catalogManager)
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

        public static void DropDatabase(string databaseName, IMongoClient _client, CatalogManager _catalogManager)
        {
            Console.WriteLine($"Dropping database: {databaseName}");
            _client?.DropDatabase(databaseName);
            _catalogManager?.DropDatabase(databaseName);
        }

        public static void DropTable(string databaseName, string tableName, IMongoClient _client, CatalogManager _catalogManager)
        {
            Console.WriteLine($"Dropping table: {databaseName}.{tableName}");
            var database = _client.GetDatabase(databaseName);
            database.DropCollection(tableName);
            _catalogManager?.DropTable(databaseName, tableName);
        }

        public static void Insert(string databaseName, string tableName, string primaryKeyColumn, List<Dictionary<string, object>> rowsData, IMongoClient _client, CatalogManager _catalogManager)
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




        public static void Delete(string databaseName, string tableName, FilterDefinition<BsonDocument> filter, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var deleteResult = collection.DeleteMany(filter);
            Console.WriteLine($"Deleted count: {deleteResult.DeletedCount}");
        }

        public static void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns, bool isUnique, IMongoClient _client, CatalogManager _catalogManager)
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


        public static void DropIndex(string databaseName, string tableName, string indexName, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            collection?.Indexes.DropOne(indexName);

            _catalogManager?.DropIndex(databaseName, tableName, indexName);
        }
    }
}
