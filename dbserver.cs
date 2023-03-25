using MongoDB.Driver;
using MongoDB.Bson;

public class DatabaseServer
{
    private IMongoClient _client;
    private IMongoDatabase _metadataDatabase;

    public DatabaseServer(string connectionString)
    {
        _client = new MongoClient(connectionString);
        _metadataDatabase = _client.GetDatabase("metadata");
    }

    public void CreateDatabase(string databaseName)
    {
        _metadataDatabase.GetCollection<BsonDocument>(databaseName).InsertOne(new BsonDocument());
    }

    public void CreateTable(string databaseName, string tableName, BsonDocument columns)
    {
        var collection = _metadataDatabase.GetCollection<BsonDocument>(databaseName);
        var table = new BsonDocument
        {
            { "name", tableName },
            { "columns", columns }
        };
        collection.InsertOne(table);
    }

    public void DropDatabase(string databaseName)
    {
        _client.DropDatabase(databaseName);
        _metadataDatabase.GetCollection<BsonDocument>(databaseName).DeleteOne(new BsonDocument());
    }

    public void DropTable(string databaseName, string tableName)
    {
        var collection = _metadataDatabase.GetCollection<BsonDocument>(databaseName);
        var filter = Builders<BsonDocument>.Filter.Eq("name", tableName);
        collection.DeleteOne(filter);
    }

    public void CreateIndex(string databaseName, string tableName, string indexName, BsonArray columns)
    {
        var collection = _metadataDatabase.GetCollection<BsonDocument>(databaseName);
        var filter = Builders<BsonDocument>.Filter.Eq("name", tableName);
        var update = Builders<BsonDocument>.Update.Set($"indexes.{indexName}", columns);
        collection.UpdateOne(filter, update);
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
                CreateTable(createStatement.DatabaseName, createStatement.TableName, BsonDocument.Parse(createStatement.Columns.ToString()));
            }
            else if (createStatement.CreateType == CreateType.Index)
            {
                CreateIndex(createStatement.DatabaseName, createStatement.TableName, createStatement.IndexName, BsonArray.Parse(createStatement.Columns.ToString()));
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