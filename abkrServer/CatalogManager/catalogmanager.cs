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


        public void CreateTable(string databaseName, string tableName, Dictionary<string, object> columns, string primaryKeyColumn, Dictionary<string, string> foreignKeys, List<string> uniqueKeys)
        {
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName);
            if (tableElement == null)
            {
                tableElement = new XElement("Table", new XAttribute("tableName", tableName));
                var structureElement = new XElement("Structure");
                foreach (var column in columns)
                {
                    XElement attribute = new XElement("Attribute");
                    attribute.SetAttributeValue("attributeName", column.Key);
                    attribute.SetAttributeValue("type", column.Value);

                    if (column.Key == primaryKeyColumn && !string.IsNullOrEmpty(primaryKeyColumn))
                    {
                        attribute.SetAttributeValue("isPrimaryKey", "true");
                        Console.WriteLine($"Primary key attribute added for column: {column.Key}");
                    }

                    // Check if the column is a foreign key
                    if (foreignKeys.ContainsKey(column.Key))
                    {
                        attribute.SetAttributeValue("isForeignKey", "true");
                        attribute.SetAttributeValue("references", foreignKeys[column.Key]);
                        Console.WriteLine($"Foreign key attribute added for column: {column.Key}");
                    }

                    // Check if the column is a unique key
                    if (uniqueKeys.Contains(column.Key))
                    {
                        attribute.SetAttributeValue("isUnique", "true");
                        Console.WriteLine($"Unique key attribute added for column: {column.Key}");
                    }

                    structureElement.Add(attribute);
                }

                var primaryKeyElement = new XElement("primaryKey", new XAttribute("name", primaryKeyColumn));
                tableElement.Add(structureElement);
                tableElement.Add(primaryKeyElement); // Add primary key element to table
                databaseElement.Add(tableElement);
                SaveMetadata(metadata);
            }
            else
            {
                throw new ArgumentException($"Table '{tableName}' already exists in database '{databaseName}'.");
            }
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
