using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

public class CatalogManager
{
    private string _metadataFilePath;

    public CatalogManager(string metadataFilePath)
    {
        _metadataFilePath = metadataFilePath;
    }

    private Dictionary<string, object> LoadMetadata()
    {
        if (File.Exists(_metadataFilePath))
        {
            string json = File.ReadAllText(_metadataFilePath);
            return JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
        }
        else
        {
            return new Dictionary<string, object>();
        }
    }

    private void SaveMetadata(Dictionary<string, object> metadata)
    {
        string json = JsonConvert.SerializeObject(metadata, Formatting.Indented);
        File.WriteAllText(_metadataFilePath, json);
    }

    public void CreateDatabase(string databaseName)
    {
        var metadata = LoadMetadata();
        metadata[databaseName] = new Dictionary<string, object>();
        SaveMetadata(metadata);
    }

    public void CreateTable(string databaseName, string tableName, Dictionary<string, object> columns)
    {
        var metadata = LoadMetadata();

        var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
        databaseMetadata[tableName] = columns;

        SaveMetadata(metadata);
    }

    public void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns)
    {
        var metadata = LoadMetadata();
        var databaseMetadata = metadata[databaseName] as Dictionary<string, object>;
        var tableMetadata = databaseMetadata[tableName] as Dictionary<string, object>;
        if (!tableMetadata.ContainsKey("indexes"))
        {
            tableMetadata["indexes"] = new Dictionary<string, object>();
        }
        var indexes = tableMetadata["indexes"] as Dictionary<string, object>;
        indexes[indexName] = columns;
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
