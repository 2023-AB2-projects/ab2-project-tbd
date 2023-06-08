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

            logger.LogMessage($"Creating index: {databaseName}.{tableName}.{indexName}");

            if(collection == null)
            {
                throw new Exception($"RecordManager.Createindex: collection does not exist for table {tableName} in database {databaseName}");
            }
            if(indexCollection == null)
            {
                _client?.GetDatabase(databaseName).CreateCollection(tableName + "_" + columns[0] + "_index");
                indexCollection = _client?.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + columns[0] + "_index");
            }

            var pos = _catalogManager.GetColumnPosition(databaseName, tableName, columns[0]);

            var docs = collection.Find(new BsonDocument()).ToList();
            if(!docs.Any())
            {
                _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns, isUnique);
                return;
            }

            var kvp = new Dictionary<string, string>();


            foreach (var doc in docs)
            {
                var values= doc.GetValue("value").AsString.Split("#");
                var value = values[(int)pos-1];
                var pk = doc["_id"].AsString;
                if (kvp[value]==null)
                {
                    kvp[value] = pk;
                }
                kvp[value] += "#"+pk;
            }

            var indexDocs = CreateIndexDocumentsFromRow(kvp);
            indexCollection.InsertMany(indexDocs);

            _catalogManager?.CreateIndex(databaseName, tableName, indexName, columns, isUnique);
        }


        public static void DropIndex(string databaseName, string tableName, string indexName, IMongoClient _client, CatalogManager _catalogManager)
        {
            _client?.GetDatabase(databaseName).DropCollection(tableName + "_" + indexName + "_index");

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

        private static List<BsonDocument> CreateIndexDocumentsFromRow(Dictionary<string, string> row)
        {
            List<BsonDocument > documents = new();
            var document = new BsonDocument();
            foreach (var kvp in row)
            {
                document["_id"] = kvp.Key;
                document["value"] = kvp.Value;
            }
            documents.Add(document);
            
            return documents;
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
            indexDocument["value"]=row.Keys.First();

            logger.LogMessage($"InsertIntoIndexCollection: indexDocument is {indexDocument.ToJson()}");

            var filter = Builders<BsonDocument>.Filter.Eq("_id", row[index.Columns[0]].ToString());
            var prevDoc = indexCollection.Find(filter).FirstOrDefault();
            if (prevDoc != null)
            {
                prevDoc["value"] += "#" + row.Keys.First();
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
            var filter = Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]);
            var result = collection.DeleteOne(filter);
            logger.LogMessage($"RecordManager.DeleteDoc: Deleted from main collection: {result}");

            // Delete from index collections
            
            var indexes = catalogManager.GetIndexes(databaseName, tableName);
            if (indexes.Count > 0)
            {
                foreach (var index in indexes)
                {
                    var indexCollection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName + "_" + index.Columns[0] + "_index");
                    if(indexCollection != null) { continue; }
                    var row = ConvertDocumentToRow(doc, catalogManager, databaseName, tableName);
                    if (index.Columns[0] == catalogManager.GetPrimaryKeyColumn(databaseName, tableName))
                    {
                        continue;
                    }

                    var indexDocumentFilter = Builders<BsonDocument>.Filter.Eq("_id", row[index.Columns[0]]);
                    var indexDocument = indexCollection.DeleteMany(indexDocumentFilter);

                    //logger.LogMessage($"RecordManager.DeleteDoc: indexDocument is {indexDocument} for index {index.Name}");

                    //if (indexDocument != null)
                    //{
                    //    var indexValues = indexDocument["value"].AsString.Split('#').ToList();
                    //    var rowIndexValues = index.Columns.Select(columnName => row[columnName].ToString());
                    //    var rowIndexValue = string.Join("&", rowIndexValues);
                    //    indexValues.Remove(rowIndexValue);
                    //    indexDocument["value"] = string.Join("#", indexValues);
                    //    indexCollection.ReplaceOne(indexDocumentFilter, indexDocument, new ReplaceOptions { IsUpsert = true });
                    //}
                    //else
                    //{
                    //    throw new Exception($"RecordManager.DeleteDoc: Delete failed. Index document not found for index {index.Name} in table {tableName} in database {databaseName}.");
                    //}
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


        // Function to retrieve documents that satisfy conditions
        public static List<BsonDocument> GetDocumentsSatisfyingConditions(string databaseName, string tableName, List<FilterCondition> conditions, IMongoClient _client, CatalogManager _catalogManager)
        {
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
            var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
            var documents = collection.Find(new BsonDocument()).ToList();

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
            //logger.LogMessage($"RecordManager.ConvertDocumentToRow: columns are: ");
            //foreach (var column in columns)
            //{
            //    logger.LogMessage(column);
            //}

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

            //logger.LogMessage($"RecordManager.ConvertDocumentToRow: Converted document {document.ToJson()} to row {string.Join(",", row)}");

            return row;
        }

        // Function to check if a row violates a foreign key constraint
        private static bool CheckForeignKeyForDelete(string databaseName, ForeignKey reference, Dictionary<string, object> row, IMongoClient _client, CatalogManager catalogManager)
        {
            //logger.LogMessage($"RecordManager.CheckForeignKeyForDelete: Checking foreign key constraint {reference.ColumnName} in table {reference.TableName} referencing {reference.ReferencedColumn} in table {reference.ReferencedTable} for row {string.Join(",", row)}");

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
