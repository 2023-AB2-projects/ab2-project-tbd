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
            foreach (var column in columns)
            {
                if (column.IsUnique)
                {
                    CreateUniqueIndex(databaseName, tableName, column.Name, _client, _catalogManager);
                }
                else
                {
                    CreateNonUniqueIndex(databaseName, tableName, column.Name, primaryKeyColumn, _client, _catalogManager);
                }
            }

            // If there's a foreign key column, create an index for it.
            foreach (var foreignKey in foreignKeys)
            {
                Console.WriteLine($"Creating foreign key:{foreignKey.Key}");
                CreateIndex(databaseName, tableName, $"{foreignKey.Key}_fk", new BsonArray(new[] { foreignKey.Key }), false, _client, _catalogManager);
            }
        }

        public static void CreateUniqueIndex(string databaseName, string tableName, string indexColumnName, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            var indexKeys = Builders<BsonDocument>.IndexKeys.Ascending(indexColumnName);
            var indexOptions = new CreateIndexOptions { Name = indexColumnName, Unique = true };
            collection?.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(indexKeys, indexOptions));

            _catalogManager?.CreateIndex(databaseName, tableName, indexColumnName, new List<string> { indexColumnName }, true);
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

            _catalogManager?.CreateIndex(databaseName, tableName, indexColumnName, new List<string> { indexColumnName }, false);
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

        public static void Insert(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            var indexList = _catalogManager.GetIndexes(databaseName, tableName);

            foreach (var index in indexList)
            {
                foreach (var column in index.Columns)
                {
                    if (!row.ContainsKey(column))
                    {
                        throw new ArgumentException($"Column '{column}' is part of index '{index.Name}' but it is not present in the row data.");
                    }
                }
            }

            foreach (var index in indexList.Where(index => index.IsUnique))
            {
                var filter = Builders<BsonDocument>.Filter.Eq("_id", Convert.ToInt32(row[index.Columns[0]]));
                var existingDocument = collection.Find(filter).FirstOrDefault();
                if (existingDocument != null)
                {
                    throw new Exception($"Insert failed. Unique index constraint violated on column '{index.Columns[0]}' in table '{tableName}' in database '{databaseName}'.");
                }
            }

            var document = new BsonDocument();
            var id = row.Keys.First();
            document["_id"] = Convert.ToInt32(row[id]);
            var otherValues = row.Where(kvp => kvp.Key != id).Select(kvp => kvp.Value.ToString());
            document["value"] = string.Join("#", otherValues);

            collection.InsertOne(document);
        }





        public static void Delete(string databaseName, string tableName,  FilterDefinition<BsonDocument> Filter, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            var foreignKeyReferences = _catalogManager.GetForeignKeyReferences(databaseName, tableName);
            foreach (var reference in foreignKeyReferences)
            {
                var refCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(reference.TableName);
                var existingDocument = refCollection.Find(Filter).FirstOrDefault();

                if (existingDocument != null)
                {
                    throw new Exception($"Delete failed. Foreign key constraint violated in table '{reference.TableName}' in database '{databaseName}' on column '{reference.ColumnName}'.");
                }
            }

            collection.DeleteOne(Filter);
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
