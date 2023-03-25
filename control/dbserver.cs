using MongoDB.Driver;
using MongoDB.Bson;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

public class DatabaseServer
{
    private IMongoClient _client;
    private CatalogManager _catalogManager;

    public DatabaseServer(string connectionString, string metadataFilePath)
    {
        _client = new MongoClient(connectionString);
        _catalogManager = new CatalogManager(metadataFilePath);
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
        var indexKeys = new IndexKeysDefinitionBuilder<BsonDocument>().Ascending(columns.Select(column => column.AsString));
        collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(indexKeys, new CreateIndexOptions { Name = indexName }));

        _catalogManager.CreateIndex(databaseName, tableName, indexName, columns);
    }

    public void DropIndex(string databaseName, string tableName, string indexName)
    {
        var collection = _client.GetDatabase(databaseName).GetCollection<BsonDocument>(tableName);
        collection.Indexes.DropOne(indexName);

        _catalogManager.DropIndex(databaseName, tableName, indexName);
    }

    public void ExecuteStatement(IStatement statement)
    {
        if (statement is CreateStatement createStatement)
        {
            if (createStatement.CreateType == CreateType.Database)
            {
                CreateDatabase(createStatement.DatabaseName);
            }
            else if (createStatement.CreateType == CreateType.Table)
            {
                CreateTable(createStatement.DatabaseName, createStatement.TableName, createStatement.AttributeDeclarations);
            }
            else if (createStatement.CreateType == CreateType.Index)
            {
                CreateIndex(createStatement.DatabaseName, createStatement.TableName, createStatement.IndexName, createStatement.Columns);
            }
        }
        else if (statement is DropStatement dropStatement)
        {
            if (dropStatement.TargetType == DropTarget.Database)
            {
                DropDatabase(dropStatement.DatabaseName);
            }
            else if (dropStatement.TargetType == DropTarget.Table)
            {
                DropTable(dropStatement.DatabaseName, dropStatement.TableName);
            }
            else if (dropStatement.TargetType == DropTarget.Index)
            {
                DropIndex(dropStatement.DatabaseName, dropStatement.TableName, dropStatement.IndexName);
            }
        }
    }
}

