using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using abkrServer.CatalogManager.RecordManager;
using abkr.ServerLogger;

namespace abkr.CatalogManager
{
    internal class RecordManager
    {
        public static Logger logger = new("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrServer/server_logger.txt");


        public static void CreateDatabase(string databaseName, IMongoClient _client, CatalogManager _catalogManager)
        {
            _client?.GetDatabase(databaseName);
            logger.LogMessage($"Creating database: {databaseName}");
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
            logger.LogMessage($"Creating table: {databaseName}.{tableName}");

            //var columnsDictionary = columns?.ToDictionary(column => column.Name, column => column.Type);
            //var primaryKeyColumn = columns.FirstOrDefault(column => column.IsPrimaryKey)?.Name
            //    ?? throw new Exception("RecordManager.CreateTable: Primary key not found.");

            _catalogManager?.CreateTable(databaseName, tableName, columns);

            // Create index files for unique keys
            foreach (var column in columns)
            {
                logger.LogMessage($"RecordManager: Creating {(column.IsUnique ? "unique" : "nonunique")} index");
                _catalogManager?.CreateIndex(databaseName, tableName, column.Name, new List<string> { column.Name }, column.IsUnique);
            }
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
            var indexCollection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName+ "_index");

            _catalogManager?.DropIndex(databaseName, tableName, indexName);
        }


        public static void DropDatabase(string databaseName, IMongoClient _client, CatalogManager _catalogManager)
        {
            logger.LogMessage($"Dropping database: {databaseName}");
            _client?.DropDatabase(databaseName);
            _catalogManager?.DropDatabase(databaseName);
        }

        public static void DropTable(string databaseName, string tableName, IMongoClient _client, CatalogManager _catalogManager)
        {
            logger.LogMessage($"Dropping table: {databaseName}.{tableName}");
            var database = _client.GetDatabase(databaseName);
            database.DropCollection(tableName);
            database.DropCollection(tableName + "_index");
            _catalogManager?.DropTable(databaseName, tableName);
        }

        public static void Insert(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            CheckUniqueAlias(databaseName, tableName, row, _client, _catalogManager);
            CheckForeignKeys(databaseName, tableName, row, _client, _catalogManager);

            InsertIntoMainCollection(databaseName, tableName, row, collection);
            InsertIntoIndexCollections(databaseName, tableName, row, _client, _catalogManager);
        }

        private static void CheckUniqueAlias(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            if (!CheckUnique(databaseName, tableName, row, _client, _catalogManager))
            {
                throw new Exception($"Insert failed. Unique index constraint violated in table '{tableName}' in database '{databaseName}'.");
            }
        }

        private static void CheckForeignKeys(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var foreignKeys = _catalogManager.GetForeignKeyReferences(databaseName, tableName);

            foreach (var foreignKey in foreignKeys)
            {
                CheckForeignKey(databaseName, foreignKey, row, _client);
            }
        }

        private static void CheckForeignKey(string databaseName, ForeignKey foreignKey, Dictionary<string, object> row, IMongoClient _client)
        {
            var referencedTable = foreignKey.ReferencedTable;
            var foreignKeyValue = row[foreignKey.ColumnName];

            var referencedCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(referencedTable);
            var filter = Builders<BsonDocument>.Filter.Eq("_id", foreignKeyValue);
            var count = referencedCollection.CountDocuments(filter);

            if (count == 0)
            {
                throw new Exception($"RecordManager.Insert failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
            }
        }

        private static void InsertIntoMainCollection(string databaseName, string tableName, Dictionary<string, object> row, IMongoCollection<BsonDocument> collection)
        {
            var document = CreateDocumentFromRow(row);
            collection.InsertOne(document);
        }

        private static BsonDocument CreateDocumentFromRow(Dictionary<string, object> row)
        {
            var document = new BsonDocument();
            var idKey = row.Keys.First();
            document["_id"] = Convert.ToInt32(row[idKey]);
            var otherValues = row.Where(kvp => kvp.Key != idKey).Select(kvp => kvp.Value.ToString());
            document["value"] = string.Join("#", otherValues);
            return document;
        }

        private static void InsertIntoIndexCollections(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var indexes = _catalogManager.GetIndexes(databaseName, tableName);

            foreach (var index in indexes)
            {
                InsertIntoIndexCollection(databaseName, tableName, row, _client, index);
            }
        }

        private static void InsertIntoIndexCollection(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, Index index)
        {
            if (index.Name == row.Keys.First())
            {
                return;
            }

            var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_index");
            var indexDocument = new BsonDocument();
            indexDocument["_id"] = index.Name;

            var filter = Builders<BsonDocument>.Filter.Eq("_id", index.Name);
            var prevDoc = indexCollection.Find(filter).FirstOrDefault();

            var indexValues = index.Columns.Select(columnName => row[columnName].ToString());
            var indexValue = string.Join("&", indexValues);

            indexDocument["value"] = prevDoc != null
                ? prevDoc.GetValue("value") + "#" + indexValue
                : indexValue;

            indexCollection.ReplaceOne(filter, indexDocument, new ReplaceOptions { IsUpsert = true });
        }


        public static bool CheckUnique(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var indexes = _catalogManager.GetIndexes(databaseName, tableName);
            var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_index");

            foreach (var index in indexes)
            {
                if (index.IsUnique)
                {
                    var filter = Builders<BsonDocument>.Filter.Eq("_id", index.Name);
                    var indexDocument = indexCollection.Find(filter).FirstOrDefault();

                    var indexValues = index.Columns.Select(columnName => row[columnName].ToString());
                    var indexValue = string.Join("&", indexValues);

                    if (indexDocument != null && indexDocument.GetValue("value").AsString.Contains(indexValue))
                    {
                        return false; // uniqueness constraint is violated
                    }
                }
            }

            return true; // no uniqueness constraint is violated
        }


        //public static void Insert(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        //{
        //    var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
        //    var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_index");

        //    var idKey = row.Keys.First();
        //    var idValue = Convert.ToInt32(row[idKey]);

        //    var indexList = _catalogManager.GetIndexes(databaseName, tableName);

        //    foreach (var index in indexList)
        //    {
        //        foreach (var column in index.Columns)
        //        {
        //            if (!row.ContainsKey(column))
        //            {
        //                throw new ArgumentException($"Column '{column}' is part of index '{index.Name}' but it is not present in the row data.");
        //            }
        //        }
        //    }
        //    var filter = Builders<BsonDocument>.Filter.Eq("_id", Convert.ToInt32(row[indexList.FirstOrDefault(index => index.IsUnique).Columns[0]]));
        //    var existingDocument = collection.Find(filter);
        //    var frequency= new Dictionary<string, int>();

        //    foreach (var kvp in existingDocument.ToList())
        //    {

        //        var values=kvp.Values.ToList();
        //        foreach(var value in values)
        //        {
        //            if (frequency.ContainsKey(value.ToString()))
        //            {
        //                frequency[value.ToString()]++;
        //            }
        //            else
        //            {
        //                frequency[value.ToString()] = 1;
        //            }
        //        }
        //        if ()
        //        //if (existingDocument != null)
        //        //{
        //        //    throw new Exception($"Insert failed. Unique index constraint violated on column '{index.Columns[0]}' in table '{tableName}' in database '{databaseName}'.");
        //        //}
        //    }

        //    var document = new BsonDocument();
        //    var id = row.Keys.First();
        //    document["_id"] = Convert.ToInt32(row[id]);
        //    var otherValues = row.Where(kvp => kvp.Key != id).Select(kvp => kvp.Value.ToString());
        //    document["value"] = string.Join("#", otherValues);

        //    collection.InsertOne(document);
        //}

        public static void Delete(string databaseName, string tableName, FilterDefinition<BsonDocument> Filter, IMongoClient _client, CatalogManager _catalogManager)
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



       
    }
}
