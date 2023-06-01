using abkrServer.CatalogManager.RecordManager;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace abkr.CatalogManager
{
    public class CatalogManager
    {
        private readonly string _metadataFilePath;

        public CatalogManager(string metadataFilePath)
        {
            _metadataFilePath = metadataFilePath;
        }

        public XElement LoadMetadata()
        {
            if (!File.Exists(_metadataFilePath))
            {
                Console.WriteLine($"Metadata file not found at {_metadataFilePath}. Creating a new file.");
                var databasesElement = new XElement("Databases");
                databasesElement.Save(_metadataFilePath);
            }

            try
            {
                return XElement.Load(_metadataFilePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading metadata file: {ex.Message}");
                return new XElement("Databases");
            }
        }

        public void SaveMetadata(XElement metadata)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_metadataFilePath));
            metadata.Save(_metadataFilePath);
            Console.WriteLine($"Metadata saved to {_metadataFilePath}");
        }

        public List<Index> GetIndexes(string databaseName, string tableName)
        {
            var indexes = new List<Index>();
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName);
            if (tableElement == null)
            {
                throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");
            }

            var indexFilesElement = tableElement.Element("IndexFiles");
            if (indexFilesElement != null)
            {
                foreach (var indexElement in indexFilesElement.Elements("IndexFile"))
                {
                    var indexName = indexElement.Attribute("indexName")?.Value;
                    var isUnique = bool.Parse(indexElement.Attribute("isUnique")?.Value ?? "false");

                    var index = new Index(indexName)
                    {
                        IsUnique = isUnique
                    };

                    var indexAttributesElement = indexElement.Element("IndexAttributes");
                    if (indexAttributesElement != null)
                    {
                        foreach (var attribute in indexAttributesElement.Elements("IAttribute"))
                        {
                            index.Columns.Add(attribute.Value);
                        }
                    }

                    indexes.Add(index);
                }
            }

            return indexes;
        }


        public List<ForeignKey> GetForeignKeyReferences(string databaseName, string tableName)
        {
            var foreignKeys = new List<ForeignKey>();
            var metadata = LoadMetadata();

            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName)
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            var structureElement = tableElement.Element("Structure");
            if (structureElement != null)
            {
                foreach (var attributeElement in structureElement.Elements("Attribute"))
                {
                    var isForeignKey = attributeElement.Attribute("isForeignKey")?.Value == "true";
                    if (isForeignKey)
                    {
                        var attributeName = attributeElement.Attribute("attributeName")?.Value;
                        var references = attributeElement.Attribute("references")?.Value;
                        if (!string.IsNullOrEmpty(attributeName) && !string.IsNullOrEmpty(references))
                        {
                            // Assumes the references string is in format "referencedTable:referencedColumn"
                            var splitReferences = references.Split(':');
                            if (splitReferences.Length == 2)
                            {
                                foreignKeys.Add(new ForeignKey
                                {
                                    TableName = tableName,
                                    ColumnName = attributeName,
                                    ReferencedTable = splitReferences[0],
                                    ReferencedColumn = splitReferences[1]
                                });
                            }
                        }
                    }
                }
            }

            return foreignKeys;
        }


        public void CreateDatabase(string databaseName)
        {
            var metadata = LoadMetadata();
            var databaseElement = new XElement("DataBase", new XAttribute("dataBaseName", databaseName));
            metadata.Add(databaseElement);
            SaveMetadata(metadata);
            Console.WriteLine($"Database '{databaseName}' created.");
        }

        public string GetPrimaryKeyColumn(string databaseName, string tableName)
        {
            XElement? metadata = LoadMetadata();
            XElement? databaseElement = metadata?.Elements("DataBase").FirstOrDefault(db => db.Attribute("dataBaseName")?.Value == databaseName);
            XElement? tableElement = databaseElement?.Elements("Table").FirstOrDefault(tbl => tbl.Attribute("tableName")?.Value == tableName);

            if (tableElement == null)
            {
                throw new Exception($"ERROR: Table {tableName} not found in database {databaseName}!");
            }

            XElement? primaryKeyAttribute = tableElement.Elements("Structure").Elements("Attribute")
                .FirstOrDefault(attr => attr.Attribute("isPrimaryKey")?.Value == "true");

            if (primaryKeyAttribute == null)
            {
                throw new Exception($"ERROR: Primary key column not found in table {tableName}!");
            }

            return primaryKeyAttribute.Attribute("attributeName")?.Value
                ?? throw new Exception($"ERROR: Primary key column name not found in table {tableName}!");
        }


        public void CreateTable(
            string databaseName,
            string tableName,
            List<Column> columns,
            string primaryKeyColumn,
            Dictionary<string, string> foreignKeys,
            List<string> uniqueKeys)
        {
            // First, check the databaseName, tableName, and columns aren't null or empty
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
                throw new ArgumentException("Columns dictionary cannot be null or empty.", nameof(columns));
            }

            // Load the existing metadata
            var metadata = LoadMetadata();

            // Ensure the database exists
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            Console.WriteLine($"CatalogManagar.CreateTable: Creating table '{tableName}' in database '{databaseName}'.");

            // Check if the table already exists
            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
            if (tableElement != null)
            {
                throw new ArgumentException($"Table '{tableName}' already exists in database '{databaseName}'.");
            }

            Console.WriteLine("Proceeding.");

            // If the table doesn't exist, create a new one
            tableElement = new XElement("Table", new XAttribute("tableName", tableName));


            // Create a new structure element
            var structureElement = new XElement("Structure");
            int i = 0;
            // Iterate through the provided columns
            foreach (var column in columns)
            {
                Console.WriteLine($"CatalogManager.CreateTable: Adding column '{column.Name}' to table '{tableName}' in iteration {i++}.");
                XElement attribute = new XElement("Attribute");
                attribute.SetAttributeValue("attributeName", column.Name);

                // Ensure the column type isn't null
                if (column.Type == null)
                {
                    throw new ArgumentException($"Column '{column.Name}' does not have a type.", nameof(columns));
                }

                attribute.SetAttributeValue("type", column.Type);

                // Check if the column is the primary key
                if (column.IsPrimaryKey)
                {
                    attribute.SetAttributeValue("isPrimaryKey", "true");
                    attribute.SetAttributeValue("isUnique", "true");
                    Console.WriteLine($"Primary key attribute added for column: {column.Name}");
                    Console.WriteLine($"Unique key attribute added for primary key: {column.Name}");
                }

                // Check if the column is a foreign key
                if (column.ForeignKeyReference!=null)
                {
                    attribute.SetAttributeValue("isForeignKey", "true");
                    attribute.SetAttributeValue("references", column.ForeignKeyReference);
                    Console.WriteLine($"Foreign key attribute added for column: {column.Name}");
                }

                // Check if the column is a unique key
                if (column.IsUnique)
                {
                    attribute.SetAttributeValue("isUnique", "true");
                    Console.WriteLine($"Unique key attribute added for column: {column.Name}");
                }

                structureElement.Add(attribute);
            }

            Console.WriteLine("CatalogManager.CreateTable: Adding primary key element.");

            if (string.IsNullOrEmpty(primaryKeyColumn))
            {
                throw new ArgumentNullException(nameof(primaryKeyColumn), "Primary key column cannot be null or empty.");
            }

            var primaryKeyElement = new XElement("primaryKey", new XAttribute("name", primaryKeyColumn));
            tableElement.Add(structureElement);
            tableElement.Add(primaryKeyElement); // Add primary key element to table
            databaseElement.Add(tableElement);

            Console.WriteLine("CatalogManager.CreateTable: Saving metadata.");

            SaveMetadata(metadata);
        }



        public void CreateIndex(string databaseName, string tableName, string indexName, List<string> columns, bool isUnique)
        {
            var metadata = LoadMetadata();

            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName) 
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName) 
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            var indexFilesElement = tableElement.Element("IndexFiles");
            if (indexFilesElement == null)
            {
                indexFilesElement = new XElement("IndexFiles");
                tableElement.Add(indexFilesElement);
            }

            var indexElement = new XElement("IndexFile", new XAttribute("indexName", indexName), new XAttribute("isUnique", isUnique));
            var indexAttributesElement = new XElement("IndexAttributes");

            foreach (var column in columns)
            {
                indexAttributesElement.Add(new XElement("IAttribute", column));
            }

            indexElement.Add(indexAttributesElement);
            indexFilesElement.Add(indexElement);
            SaveMetadata(metadata);
        }

        public void DropIndex(string databaseName, string tableName, string indexName)
        {
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName) 
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            var indexFilesElement = tableElement.Element("IndexFiles");
            var indexElement = indexFilesElement?.Elements("IndexFile").FirstOrDefault(e => e.Attribute("indexName")?.Value == indexName);

            if (indexElement != null)
            {
                indexElement.Remove();
                SaveMetadata(metadata);
            }
            else
            {
                throw new ArgumentException($"Index '{indexName}' does not exist in table '{tableName}'.");
            }
        }

        public void DropDatabase(string databaseName)
        {
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName);

            if (databaseElement != null)
            {
                databaseElement.Remove();
                SaveMetadata(metadata);
            }
            else
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }
        }

        public void DropTable(string databaseName, string tableName)
        {
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName);
            if (tableElement != null)
            {
                tableElement.Remove();
                SaveMetadata(metadata);
            }
            else
            {
                throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");
            }
        }

        public bool IsUniqueKey(string databaseName, string tableName, string columnName)
        {
            XElement? metadata = LoadMetadata();
            XElement? databaseElement = metadata?.Elements("DataBase").FirstOrDefault(db => db.Attribute("dataBaseName")?.Value == databaseName);
            XElement? tableElement = databaseElement?.Elements("Table").FirstOrDefault(tbl => tbl.Attribute("tableName")?.Value == tableName);

            if (tableElement == null)
            {
                throw new Exception($"ERROR: Table {tableName} not found in database {databaseName}!");
            }

            XElement? columnAttribute = tableElement.Elements("Structure").Elements("Attribute")
                .FirstOrDefault(attr => attr.Attribute("attributeName")?.Value == columnName);

            if (columnAttribute == null)
            {
                throw new Exception($"ERROR: Column {columnName} not found in table {tableName}!");
            }

            var isUniqueAttribute = columnAttribute.Attribute("isUnique")?.Value;

            return isUniqueAttribute == "true";
        }

    }
}
