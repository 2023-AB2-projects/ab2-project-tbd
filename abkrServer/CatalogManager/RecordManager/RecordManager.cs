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
using Microsoft.Extensions.Logging;

namespace abkr.CatalogManager
{
    internal class RecordManager
    {
        public static Logger logger = new("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrServer/server_logger.log");


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
            //foreach (var column in columns)
            //{
            //    logger.LogMessage($"RecordManager: Creating {(column.IsUnique ? "unique" : "nonunique")} index");
            //    _catalogManager?.CreateIndex(databaseName, tableName, column.Name, new List<string> { column.Name }, column.IsUnique);
            //}
        }

        public static void CreateIndex(string databaseName, string tableName, string indexName, List<string> columns, bool isUnique, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var indexCollection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + columns[0] + "_index");

            if(collection == null)
            {
                throw new Exception($"RecordManager.Createindex: collection does not exist for table {tableName} in database {databaseName}");
            }
            if(indexCollection == null)
            {
                _client?.GetDatabase(databaseName).CreateCollection(tableName + "_" + columns[0] + "_index");
                indexCollection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + columns[0] + "_index");
            }

            var pos = _catalogManager.GetColumnPosition(databaseName, tableName, columns[0])
                ??throw new Exception($"RecordManager.Createindex: column {columns[0]} not found in table {tableName} in database {databaseName}");

            var docs = collection.Find(new BsonDocument()).ToList();
            if(!docs.Any())
            {
                _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns, isUnique);
                return;
            }

            var kvp = new Dictionary<string, string>();

            foreach (var doc in docs)
            {
                //logger.LogMessage($"doc: {doc.ToJson()}");
                var values= doc.GetValue("value").AsString.Split("#");
                //logger.LogMessage($"values: ");

                //foreach (var v in values)
                //{
                //    logger.LogMessage($"{v}");
                //}
                var value = values[(int)pos-2];
                //logger.LogMessage($"value: {value}");
                var pkValue = doc["_id"].ToString();
                if (!kvp.ContainsKey(value))
                {
                    kvp[value] = pkValue;
                }
                else
                {
                    kvp[value] += "#" + pkValue;
                }
            }

            var indexDocs = CreateIndexDocumentsFromRow(kvp);
            foreach(var doc in indexDocs)
            {
                indexCollection.InsertOne(doc);
            }

            _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns, isUnique);
        }

        private static List<BsonDocument> CreateIndexDocumentsFromRow(Dictionary<string, string> row)
        {
            List<BsonDocument> documents = new();
            foreach (var kvp in row)
            {
                var document = new BsonDocument
                {
                    ["_id"] = kvp.Key,
                    ["value"] = kvp.Value
                };
                documents.Add(document);
            }

            return documents;
        }


        public static void DropIndex(string databaseName, string tableName, string indexName, IMongoClient _client, CatalogManager _catalogManager)
        {
            _client?.GetDatabase(databaseName).DropCollection(tableName + "_" + indexName);

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
            //logger.LogMessage($"Dropping table: {databaseName}.{tableName}");
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

        public static void Update(string databaseName, string tableName, List<FilterCondition> conditions, Dictionary<string, object> newRow, IMongoClient _client, CatalogManager _catalogManager)
        {
            // Check if we're updating a foreign key
            var foreignKeys = _catalogManager.GetForeignKeyReferences(databaseName, tableName);
            foreach (var foreignKey in foreignKeys)
            {
                if (newRow.ContainsKey(foreignKey.ColumnName))
                {
                    throw new Exception($"Update failed. Cannot update a foreign key column '{foreignKey.ColumnName}' in table '{tableName}' in database '{databaseName}'.");
                }
            }

            // Get the collection from the database
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            // Create a filter for the conditions
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filters = conditions.Select(c => filterBuilder.Eq(c.ColumnName, c.Value)).ToList();
            var filter = filterBuilder.And(filters);

            // Get the old document first to fetch its index values
            var oldDocument = collection.Find(filter).FirstOrDefault();
            if (oldDocument == null)
            {
                throw new Exception($"No row found to update in table '{tableName}' in database '{databaseName}' with the provided conditions.");
            }

            // Create the update definition for the main data
            var updateDefinition = Builders<BsonDocument>.Update;
            var updateDefinitionList = newRow.Select(pair => updateDefinition.Set(pair.Key, pair.Value.ToString())).ToList();
            var update = updateDefinition.Combine(updateDefinitionList);

            // Update the document
            collection.UpdateOne(filter, update);

            // Now, update the index
            var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>("IndexTable");
            var indexes = _catalogManager.GetIndexes(databaseName, tableName);
            foreach (var updatedValue in newRow)
            {
                // Check if the updated field is part of the index
                if (indexes.Any(index => index.Columns.Contains(updatedValue.Key)))
                {
                    // Find the index for the old value and update it
                    var indexFilter = Builders<BsonDocument>.Filter.Eq("columnName", updatedValue.Key) & Builders<BsonDocument>.Filter.Eq("rowId", oldDocument.GetValue("_id"));
                    var indexUpdate = Builders<BsonDocument>.Update.Set("indexedValue", updatedValue.Value.ToString());

                    indexCollection.UpdateOne(indexFilter, indexUpdate);
                }
            }
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
            //logger.LogMessage($"RecordManager.CheckForeignKey: foreignKeyValue is {foreignKeyValue} for column {foreignKey.ColumnName}");

            if (foreignKey.ReferencedColumn.Item2)
            {
                var referencedCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(referencedTable);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", Convert.ToInt32(foreignKeyValue));
                _ = referencedCollection.Find(filter).FirstOrDefault()
                    ?? throw new Exception($"RecordManager.CheckForeignKey failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
            }
            else
            {
                var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(referencedTable + "_" + foreignKey.ReferencedColumn + "_index");
                if (indexCollection != null) 
                { 
                    return;

                }
               
                var filter = Builders<BsonDocument>.Filter.Eq("_id", foreignKeyValue);
                var doc = indexCollection.Find(filter).FirstOrDefault() 
                    ?? throw new Exception($"RecordManager.CheckForeignKey failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
                //var count = doc.GetValue("value").AsString.Split('#');
                ////logger.LogMessage($"Count: {count.Length}");
                //if (!count.Contains(foreignKeyValue.ToString()))
                //{
                //    throw new Exception($"RecordManager.CheckForeignKey failed. Foreign key constraint violated on column '{foreignKey.ColumnName}' in table '{foreignKey.TableName}' in database '{databaseName}'. Referenced record not found in table '{referencedTable}' for column '{foreignKey.ReferencedColumn}'.");
                //}
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
            if (index.Columns[0] == row.Keys.First() || index==null)
            {
                return;
            }

            var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + index.Columns[0] + "_index");

            var indexDocument = new BsonDocument();
            indexDocument["_id"] = row[index.Columns[0]].ToString();
            indexDocument["value"]=row[row.Keys.First()].ToString();

            var filter = Builders<BsonDocument>.Filter.Eq("_id", row[index.Columns[0]].ToString());
            var prevDoc = indexCollection.Find(filter).FirstOrDefault();
            if (prevDoc != null)
            {
                prevDoc["value"] += "#" + row[row.Keys.First()];
                indexCollection.InsertOne(prevDoc);
            }
            else
            {
                indexCollection.InsertOne(indexDocument);
            }
        }


        public static bool CheckUnique(string databaseName, string tableName, Dictionary<string, object> row, IMongoClient _client, CatalogManager _catalogManager)
        {
            var indexes = _catalogManager.GetIndexes(databaseName, tableName);


            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(Builders<BsonDocument>.Filter.Empty).ToEnumerable();

            foreach (var column in row.Keys)
            {
                if (indexes.Where(i => i.Columns[0] == column).Any())
                {
                    var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + column + "_index");
                    if (indexCollection != null)
                    {


                        var filter = Builders<BsonDocument>.Filter.Eq("_id", row[column]);
                        if (indexCollection.Find(filter).Any())
                        {
                            logger.LogMessage($"RecordManager.CheckUnique: Unique constraint violated on column '{column}' with already existing value of {row[column]} in table '{tableName}' in database '{databaseName}'.");
                            return false;
                        }
                    }
                }
                else
                {
                    foreach (var document in documents)
                    {
                        var pos = _catalogManager.GetColumnPosition(databaseName, tableName, column);
                        string[] values = document.GetValue("value").AsString.Split('#');
                        if (values[(int)pos - 1] == row[column])
                        {
                            logger.LogMessage($"RecordManager.CheckUnique: Unique constraint violated on column '{column}' with already existing value of {row[column]} in table '{tableName}' in database '{databaseName}'.");
                            return false;
                        }
                    }
                }
            }

            return true; // no uniqueness constraint is violated
        }

        public static void Delete(string databaseName, string tableName, List<FilterCondition> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var primaryKey = _catalogManager.GetPrimaryKeyColumn(databaseName, tableName);

            //logger.LogMessage($"RecordManager.Delete: Deleting from table {tableName} in database {databaseName} with conditions {string.Join(",", conditions.Select(c=>$"{c.ColumnName} {c.Operator} {c.Value}"))}");

            // Step 1: Retrieve documents from the main collection that satisfy the conditions
            var documents = GetDocumentsSatisfyingConditions(databaseName, tableName, conditions, _client, _catalogManager);

            // Step 2: Check foreign key constraints
            var primaryKeyCondition = conditions.FirstOrDefault(c => c.ColumnName == primaryKey);
            if (primaryKeyCondition != null)
            {
                var foreignKeyReferences = _catalogManager.GetForeignKeyReferences(databaseName, tableName);
                foreach (var reference in foreignKeyReferences)
                {
                    foreach (var document in documents)
                    {
                        var row = ConvertDocumentToRow(document,_catalogManager, databaseName, tableName);
                        var value = row[reference.ColumnName];
                        var result = CheckForeignKeyForDelete(databaseName, reference, row, _client, _catalogManager);
                        if (!result)
                        {
                            throw new Exception($"Delete failed. Foreign key constraint violated on column '{reference.ColumnName}' in table '{reference.TableName}' in database '{databaseName}'. Referenced record found in table '{reference.ReferencedTable}' for column '{reference.ReferencedColumn}'.");
                        }
                    }
                }
            }

            //logger.LogMessage($"RecordManager.Delete: Deleting documents {string.Join(",", documents)}!!! from table {tableName} in database {databaseName}");

            // Step 3: Delete records from main and index collections
            foreach (var document in documents)
            {
                DeleteDoc(databaseName, tableName, _client, collection, document, _catalogManager);
                //logger.LogMessage($"RecordManager.Delete: Deleted document {document} from table {tableName} in database {databaseName}");
            }
        }

        private static void DeleteDoc(string databaseName, string tableName, IMongoClient _client, IMongoCollection<BsonDocument> collection, BsonDocument doc,CatalogManager catalogManager)
        {
            // Delete from main collection
            //var filter = Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]);
            //var pkValue=doc.GetValue("_id").ToString();
            //var values = doc.GetValue("value").AsString.Split('#');
            var result = collection.DeleteOne(doc);
            logger.LogMessage($"RecordManager.DeleteDoc: Deleted from main collection: {result}");


            // Delete from index collections
            
            var indexes = catalogManager.GetIndexes(databaseName, tableName);
            if (indexes.Count > 0)
            {
                foreach (var index in indexes)
                {
                    var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + index.Columns[0] + "_index");
                    if(indexCollection == null) { continue; }
                    var row = ConvertDocumentToRow(doc, catalogManager, databaseName, tableName);
                    if (index.Columns[0] == catalogManager.GetPrimaryKeyColumn(databaseName, tableName))
                    {
                        continue;
                    }

                    var indexDocumentFilter = Builders<BsonDocument>.Filter.Eq("_id", row[index.Columns[0]]);
                    var indexDocument = indexCollection.DeleteMany(indexDocumentFilter);
                }
            }
        }
        
        public static bool SatisfiesConditions(Dictionary<string, object> row, List<FilterCondition> conditions)
        {
            if(row == null || !row.Any())
            {
                throw new Exception("RecordManager.SatisfiesConditions: Row is null or empty.");
            }

            foreach (var condition in conditions)
            {
                var op = condition.Operator;
                var columnValue = condition.Value;

                //logger.LogMessage($"RecordManager.SatisfiesConditions: Checking condition {condition.ColumnName} {op} {columnValue} for values {string.Join(",", row[condition.ColumnName])}");


                // Check if the row has the column specified in the condition
                if (!row.ContainsKey(condition.ColumnName))
                {
                    return false;
                }

                // Get the operator and value from the condition
               
                // Use the FilteredValues method to determine if the row satisfies the condition
                var filteredValues = FilteredValues(op, columnValue, new[] { row[condition.ColumnName].ToString() });
                if (!filteredValues.Any())
                {
                    return false;
                }
            }

            return true;
        }

        public static List<Index> GetIntersectedIndexes(List<Index> indexes, List<FilterCondition> conditions)
        {
            var intersectedIndexes = indexes
                .Where(index => index.Columns.Count > 0)
                .Where(index => conditions.Any(condition => condition.ColumnName == index.Columns[0]))
                .ToList();

            return intersectedIndexes;
        }



        // Function to retrieve documents that satisfy conditions
        public static List<BsonDocument> GetDocumentsSatisfyingConditions(string databaseName, string tableName, List<FilterCondition> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
            //var indexes = _catalogManager.GetIndexes(databaseName, tableName);
            //List<string> conditionIndexes = GetIntersectedIndexes(indexes, conditions);
           
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(new BsonDocument()).ToList();

            var result = new List<BsonDocument>();
            foreach (var document in documents)
            {
                //logger.LogMessage($"RecordManager.GetDocumentsSatisfyingConditions: Checking document {document.ToJson()} from table {tableName} in database {databaseName}");

                var row = ConvertDocumentToRow(document, _catalogManager, databaseName, tableName);
                if (SatisfiesConditions(row, conditions))
                {
                    result.Add(document);
                }
            }
            return result;
        }

        public static List<Dictionary<string, object>> GetRowsSatisfyingConditions(string databaseName, string tableName, List<FilterCondition> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
            //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Getting rows satisfying conditions {string.Join(",", conditions)} from table {tableName} in database {databaseName}");
            //IDE ELJUT!!!!!!!!!!!!!!!!!!!!
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);

            // Start with all primary keys.
            HashSet<string> primaryKeys = new HashSet<string>(collection.AsQueryable().Select(doc => doc["_id"].ToString()));


            // Get the indexes associated with the table.
            var indexes = _catalogManager.GetIndexes(databaseName, tableName);

            // Iterate over each condition
            foreach (var condition in conditions)
            {

                //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Checking condition {condition.ColumnName} {condition.Operator} {condition.Value} from table {tableName} in database {databaseName}");
                var index = indexes.FirstOrDefault(i => i.Columns.Contains(condition.ColumnName));
                if (index != null) // index exists
                {
                    var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + condition.ColumnName + "_index");

                    if(indexCollection == null) { continue; }

                    // Determine the MongoDB filter based on the operator in the condition.
                    FilterDefinition<BsonDocument> filter = condition.Operator switch
                    {
                        "=" => Builders<BsonDocument>.Filter.Eq("_id", condition.Value),
                        ">" => Builders<BsonDocument>.Filter.Gt("_id", condition.Value),
                        ">=" => Builders<BsonDocument>.Filter.Gte("_id", condition.Value),
                        "<" => Builders<BsonDocument>.Filter.Lt("_id", condition.Value),
                        "<=" => Builders<BsonDocument>.Filter.Lte("_id", condition.Value),
                        "!=" => Builders<BsonDocument>.Filter.Ne("_id", condition.Value),
                        _ => throw new ArgumentException($"Unsupported operator: {condition.Operator}"),
                    };

                    // Retrieve the documents in the index that satisfy the condition.
                    var filteredDocuments = indexCollection.Find(filter).ToList();

                    //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Found {filteredDocuments.Count} documents satisfying condition {condition.ColumnName} {condition.Operator} {condition.Value} from table {tableName} in database {databaseName}");

                    // Extract the primary keys from the filtered documents.
                    HashSet<string> filteredPrimaryKeys = new(filteredDocuments.Select(doc => doc["value"].AsString));

                    //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Found {filteredPrimaryKeys.Count} primary keys satisfying condition {condition.ColumnName} {condition.Operator} {condition.Value} from table {tableName} in database {databaseName}");

                    // Update primaryKeys to be the intersection of itself and filteredPrimaryKeys
                    primaryKeys.IntersectWith(filteredPrimaryKeys);

                    //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Found {primaryKeys.Count} primary keys satisfying condition {condition.ColumnName} {condition.Operator} {condition.Value} from table {tableName} in database {databaseName}");
                }
            }

            // At this point, primaryKeys contains the keys of the documents that satisfy all conditions.

            // Use the remaining primary keys to retrieve the corresponding documents from the main collection.

            //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Found {primaryKeys.Count} primary keys satisfying conditions: {string.Join(',', primaryKeys.Select(pk=>pk))}.");
            var filterBuilder = Builders<BsonDocument>.Filter;
            var intValues = primaryKeys.Select(pk => Convert.ToInt32(pk));
            var primaryKeysFilter = filterBuilder.In("_id", intValues);
            var finalDocuments = collection.Find(primaryKeysFilter).ToList();

            //logger.LogMessage($"RecordManager.GetRowsSatisfyingConditions: Found {finalDocuments.Count} documents satisfying conditions.");

            // Convert the final documents to rows and return them.
            var finalRows = GetRowsSatisfyingConditionsAfterIndexCheck(databaseName, tableName,conditions,_catalogManager, finalDocuments);

            return finalRows;
        }

        public static List<Dictionary<string, object>> GetRowsSatisfyingConditionsAfterIndexCheck(string databaseName, string tableName, List<FilterCondition> conditions, CatalogManager _catalogManager, List<BsonDocument> documents)
        {
            //var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            //var documents = collection.Find(new BsonDocument()).ToList();


            var result = new List<Dictionary<string, object>>();
            foreach (var document in documents)
            {
                var row = ConvertDocumentToRow(document, _catalogManager, databaseName, tableName);
                if (!SatisfiesConditions(row, conditions)) continue;
                result.Add(row);
            }
            return result;
        }
      
        // Function to convert a document to a row
        public static Dictionary<string, object> ConvertDocumentToRow(BsonDocument document, CatalogManager catalogManager, string databasName, string tableName)
        {
            var row = new Dictionary<string, object>();
            var values = document["value"].AsString.Split('#');

            var columns = catalogManager.GetColumnNames(databasName, tableName);

            var pk = catalogManager.GetPrimaryKeyColumn(databasName, tableName);
            row[pk] = document["_id"];

            var i = 0;
            foreach ( var column in columns)
            {
                if (column == pk)
                {
                    continue;
                }
                row[column] = values[i++];
            }

            return row;
        }

        // Function to check if a row violates a foreign key constraint
        private static bool CheckForeignKeyForDelete(string databaseName, ForeignKey reference, Dictionary<string, object> row, IMongoClient _client, CatalogManager catalogManager)
        {
            if (!row.ContainsKey(reference.ColumnName))
            {
                return true;
            }


            var hasMainIndex = catalogManager.HasIndex(databaseName, reference.TableName, reference.ColumnName);
            var hasReferenceIndex = catalogManager.HasIndex(databaseName, reference.ReferencedTable, reference.ReferencedColumn.Item1);
            if (hasMainIndex)
            {
                if (hasReferenceIndex)
                {
                    var refIndexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(reference.ReferencedTable + "_" + reference.ReferencedColumn.Item1 + "_index");
                    var docs = refIndexCollection.Find(Builders<BsonDocument>.Filter.Eq("_id", row[reference.ColumnName]));
                    if (docs.Any())
                    {
                        logger.LogMessage($"RecordManager.CheckForeignKeyForDelete: Violation of foreign key constraint {reference.ColumnName} in table {reference.TableName} referencing {reference.ReferencedColumn} in table {reference.ReferencedTable} for row {string.Join(",", row)}");
                        return false;
                    }
                }
                else
                {
                    var IndexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(reference.TableName + "_" + reference.ColumnName + "_index");
                    var docs = IndexCollection.Find(Builders<BsonDocument>.Filter.Eq("_id", row[reference.ColumnName]));
                    if (docs.Any())
                    {
                        logger.LogMessage($"RecordManager.CheckForeignKeyForDelete: Violation of foreign key constraint {reference.ColumnName} in table {reference.TableName} referencing {reference.ReferencedColumn} in table {reference.ReferencedTable} for row {string.Join(",", row)}");
                        return false;
                    }
                }
            }
            else
            {
                var foreignKeyCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(reference.ReferencedTable);
                var filter = Builders<BsonDocument>.Filter.Empty;
                var result = foreignKeyCollection.Find(filter).ToEnumerable();
                var pos = catalogManager.GetColumnPosition(databaseName, reference.ReferencedTable, reference.ReferencedColumn.Item1);
                foreach (var entry in result)
                {
                    var values = entry["value"].AsString.Split('#');
                    var referencedColumnValue = values[(int)pos];
                    var rowColumnValue = row[reference.ColumnName].ToString();
                    //logger.LogMessage($"RecordManager.CheckForeignKeyForDelete: Checking if {referencedColumnValue} == {rowColumnValue}");
                    if (referencedColumnValue == rowColumnValue)
                    {
                        logger.LogMessage($"RecordManager.CheckForeignKeyForDelete: Violation of foreign key constraint {reference.ColumnName} in table {reference.TableName} referencing {reference.ReferencedColumn} in table {reference.ReferencedTable} for row {string.Join(",", row)}");
                        return false;
                    }
                }

                return result == null;
            }
            return true;
        
        }


        public static IEnumerable<string> FilteredValues(string op, object columnValue, string[] values)
        {
            //logger.LogMessage($"RecordManager.FilteredValues: Filtering values {string.Join(",", values)} for operator {op} and column value {columnValue}");

            return op switch
            {
                //EQUALS | GREATER_THAN | GREATER_EQUALS | LESS_THAN | LESS_EQUALS | DIFFERS;
                "=" => values.Where(v => v == columnValue.ToString()),
                ">" => values.Where(v => Convert.ToInt32(v) > Convert.ToInt32(columnValue.ToString())),
                ">=" => values.Where(v => Convert.ToInt32(v) >= Convert.ToInt32(columnValue.ToString())),
                "<" => values.Where(v => Convert.ToInt32(v) < Convert.ToInt32(columnValue.ToString())),
                "<=" => values.Where(v => Convert.ToInt32(v) <= Convert.ToInt32(columnValue.ToString())),
                "!=" => values.Where(v => v != columnValue.ToString()),
                _ => throw new ArgumentException($"Unsupported operator: {op}"),
            };
        }

    }
}
