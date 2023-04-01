using MongoDB.Bson;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

namespace abkr.CatalogManager
{
    public class CatalogManager
    {
        private string _metadataFilePath;

        public CatalogManager(string metadataFilePath)
        {
            _metadataFilePath = metadataFilePath;
        }

        public Dictionary<string, object> LoadMetadata()
        {
            if (!File.Exists(_metadataFilePath))
            {
                Console.WriteLine($"Metadata file not found at {_metadataFilePath}. Creating a new file.");
                File.WriteAllText(_metadataFilePath, "{}");
            }

            string metadataJson = File.ReadAllText(_metadataFilePath);

            if (string.IsNullOrWhiteSpace(metadataJson))
            {
                Console.WriteLine("Metadata file is empty. Returning an empty dictionary.");
                return new Dictionary<string, object>();
            }

            try
            {
                return JsonConvert.DeserializeObject<Dictionary<string, object>>(metadataJson);
            }
            catch (JsonReaderException ex)
            {
                Console.WriteLine($"Error reading metadata file: {ex.Message}");
                return new Dictionary<string, object>();
            }
        }



        public void SaveMetadata(Dictionary<string, object> metadata)
        {
            string json = JsonConvert.SerializeObject(metadata, Formatting.Indented);
            File.WriteAllText(_metadataFilePath, json);
            Console.WriteLine($"Metadata saved to {_metadataFilePath}");
        }


        public void CreateDatabase(string databaseName)
        {
            var metadata = LoadMetadata();
            metadata[databaseName] = new Dictionary<string, object>();
            SaveMetadata(metadata);
        }


        public void CreateTable(string databaseName, string tableName, Dictionary<string, string> columns)
        {
            var metadata = LoadMetadata();

            if (!metadata.ContainsKey(databaseName))
            {
                metadata[databaseName] = new Dictionary<string, object>();
            }

            var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;

            if (!databaseMetadata.ContainsKey(tableName))
            {
                databaseMetadata[tableName] = columns;
                SaveMetadata(metadata);
            }
            else
            {
                throw new ArgumentException($"Table '{tableName}' already exists in database '{databaseName}'.");
            }
        }





        public void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns, Dictionary<string, object>? indexes)
        {
            var metadata = LoadMetadata();
            var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
            var tableMetadata = databaseMetadata[tableName] as Dictionary<string, object>;
            if (!tableMetadata.ContainsKey("indexes"))
            {
                tableMetadata["indexes"] = new Dictionary<string, object>();
            }
            var indices = tableMetadata["indexes"] as Dictionary<string, object>;
            indices[indexName] = columns;
            SaveMetadata(metadata);
        }
        public void DropIndex(string databaseName, string tableName, string indexName)
        {
            var metadata = LoadMetadata();
            var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
            var tableMetadata = databaseMetadata[tableName] as Dictionary<string, object>;
            if (tableMetadata.ContainsKey("indexes"))
            {
                var indexes = tableMetadata["indexes"] as Dictionary<string, object>;
                indexes.Remove(indexName);
            }
            SaveMetadata(metadata);
        }
        public void DropDatabase(string databaseName)
        {
            var metadata = LoadMetadata();
            metadata.Remove(databaseName);
            SaveMetadata(metadata);
        }

        public void DropTable(string databaseName, string tableName)
        {
            var metadata = LoadMetadata();
            var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
            databaseMetadata.Remove(tableName);
            SaveMetadata(metadata);
        }

    }
}
