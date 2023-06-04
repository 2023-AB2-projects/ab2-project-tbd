using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using abkrServer.CatalogManager.RecordManager;
using abkr.ServerLogger;
using static OfficeOpenXml.ExcelErrorValue;

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
            logger.LogMessage($"RecordManager.CheckForeignKey: foreignKeyValue is {foreignKeyValue} for column {foreignKey.ColumnName}");

            if (foreignKey.ReferencedColumn.Item2)
            {
                var referencedCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(referencedTable);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", Convert.ToInt32(foreignKeyValue));
                _ = referencedCollection.Find(filter).FirstOrDefault()
                    ?? throw new Exception($"RecordManager.CheckForeignKey failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
            }
            else
            {
                var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(referencedTable + "_index");
                var filter = Builders<BsonDocument>.Filter.Eq("_id", foreignKey.ReferencedColumn.Item1);
                var count = indexCollection.Find(filter).FirstOrDefault().GetValue("value").AsString.Split('#');
                logger.LogMessage($"Count: {count.Length}");
                if (!count.Contains(foreignKeyValue.ToString()))
                {
                    throw new Exception($"RecordManager.CheckForeignKey failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
                }
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
                if (index.IsUnique && (index.Name != _catalogManager.GetPrimaryKeyColumn(databaseName, tableName)))
                {
                    var filter = Builders<BsonDocument>.Filter.Eq("_id", index.Name);
                    var indexDocument = indexCollection.Find(filter).FirstOrDefault();
                    logger.LogMessage($"RecordManager.CheckUnique: indexDocument is {indexDocument} for index {index.Name}");

                    var indexValues = index.Columns.Select(columnName => row[columnName].ToString());
                    var indexValue = string.Join("&", indexValues);

                    if (indexDocument != null && indexDocument.GetValue("value").AsString.Contains(indexValue))
                    {
                        logger.LogMessage($"RecordManager.CheckUnique: Unique constraint violated on column '{index.Name}' with already existing value of {row[index.Name]} in table '{tableName}' in database '{databaseName}'.");
                        return false; // uniqueness constraint is violated
                    }
                }
            }

            return true; // no uniqueness constraint is violated
        }

        public static void Delete(string databaseName, string tableName, Dictionary<string, object> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_index");
            var primaryKey = _catalogManager.GetPrimaryKeyColumn(databaseName, tableName);

            // Step 1: Retrieve documents from the main collection that satisfy the conditions
            var documents = GetDocumentsSatisfyingConditions(databaseName, tableName, conditions, _client, _catalogManager);

            // Step 2: Check foreign key constraints
            if (conditions.ContainsKey(primaryKey))
            {
                var foreignKeyReferences = _catalogManager.GetForeignKeyReferences(databaseName, tableName);
                foreach (var reference in foreignKeyReferences)
                {
                    foreach (var document in documents)
                    {
                        var row = ConvertDocumentToRow(document);
                        var value = row[reference.ColumnName];
                        var result = CheckForeignKeyForDelete(databaseName, reference, row, _client);
                        if (!result)
                        {
                            throw new Exception($"Delete failed. Foreign key constraint violated on column '{reference.ColumnName}' in table '{reference.TableName}' in database '{databaseName}'. Referenced record found in table '{reference.ReferencedTable}' for column '{reference.ReferencedColumn}'.");
                        }
                    }
                }
            }

            // Step 3: Delete records from main and index collections
            foreach (var document in documents)
            {
                // Delete from main collection
                var filter = Builders<BsonDocument>.Filter.Eq("_id", document["_id"]);
                collection.DeleteOne(filter);

                // Delete from index collections
                var row = ConvertDocumentToRow(document);
                var indexes = _catalogManager.GetIndexes(databaseName, tableName);
                foreach (var index in indexes)
                {
                    var indexDocumentFilter = Builders<BsonDocument>.Filter.Eq("_id", index.Name);
                    var indexDocument = indexCollection.Find(indexDocumentFilter).FirstOrDefault();
                    if (indexDocument != null)
                    {
                        var indexValues = indexDocument["value"].AsString.Split('#').ToList();
                        var rowIndexValues = index.Columns.Select(columnName => row[columnName].ToString());
                        var rowIndexValue = string.Join("&", rowIndexValues);
                        indexValues.Remove(rowIndexValue);
                        indexDocument["value"] = string.Join("#", indexValues);
                        indexCollection.ReplaceOne(indexDocumentFilter, indexDocument);
                    }
                }
            }
        }

        // Function to retrieve documents that satisfy conditions
        private static List<BsonDocument> GetDocumentsSatisfyingConditions(string databaseName, string tableName, Dictionary<string, object> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(new BsonDocument()).ToList();

            var result = new List<BsonDocument>();
            foreach (var document in documents)
            {
                var row = ConvertDocumentToRow(document);
                if (SatisfiesConditions(row, conditions))
                {
                    result.Add(document);
                }
            }
            return result;
        }

        // Function to convert a document to a row
        private static Dictionary<string, object> ConvertDocumentToRow(BsonDocument document)
        {
            var row = new Dictionary<string, object>();
            row["_id"] = document["_id"];
            var values = document["value"].AsString.Split('#');
            for (int i = 0; i < values.Length; i++)
            {
                var parts = values[i].Split('=');
                row[parts[0]] = parts[1];
            }
            return row;
        }

        // Function to check if a row satisfies conditions
        private static bool SatisfiesConditions(Dictionary<string, object> row, Dictionary<string, object> conditions)
        {
            foreach (var condition in conditions)
            {
                if (!row.ContainsKey(condition.Key) || row[condition.Key].ToString() != condition.Value.ToString())
                {
                    return false;
                }
            }
            return true;
        }

        // Function to check if a row violates a foreign key constraint
        private static bool CheckForeignKeyForDelete(string databaseName, ForeignKey reference, Dictionary<string, object> row, IMongoClient _client)
        {
            if (!row.ContainsKey(reference.ColumnName))
            {
                return true;
            }

            var foreignKeyCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(reference.ReferencedTable);
            var filter = Builders<BsonDocument>.Filter.Eq(reference.ReferencedColumn.Item1, row[reference.ColumnName]);
            var result = foreignKeyCollection.Find(filter).FirstOrDefault();

            return result == null;
        }


        private static IEnumerable<string> FilteredValues(string columnName, string op, object columnValue, string[] values)
        {
            switch (op)
            {
                //EQUALS | GREATER_THAN | GREATER_EQUALS | LESS_THAN | LESS_EQUALS;

                case "EQUALS":
                    return values.Where(v=> v==columnValue.ToString());
                case "GREATER_THAN":
                    return values.Where(v => Convert.ToInt32(v) > Convert.ToInt32(columnValue.ToString()));
                case "GREATER_EQUALS":
                    return values.Where(v => Convert.ToInt32(v) >= Convert.ToInt32(columnValue.ToString()));
                case "LESS_THAN":
                    return values.Where(v => Convert.ToInt32(v) < Convert.ToInt32(columnValue.ToString()));
                case "LESS_EQUALS":
                    return values.Where(v => Convert.ToInt32(v) <= Convert.ToInt32(columnValue.ToString()));
                default:
                    throw new ArgumentException($"Unsupported operator: {op}");
            }
        }

    }
}
