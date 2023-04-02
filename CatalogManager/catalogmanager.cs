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
        private string _metadataFilePath;

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

        public void CreateTable(string databaseName, string tableName, Dictionary<string, string> columns)
        {
            var metadata = LoadMetadata();
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
            if (tableElement == null)
            {
                tableElement = new XElement("Table", new XAttribute("tableName", tableName));
                var structureElement = new XElement("Structure");
                foreach (var column in columns)
                {
                    structureElement.Add(new XElement("Attribute", new XAttribute("attributeName", column.Key), new XAttribute("type", column.Value)));
                }
                tableElement.Add(structureElement);
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
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
            if (tableElement == null)
            {
                throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");
            }

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
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
            if (tableElement == null)
            {
                throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");
            }

            var indexFilesElement = tableElement.Element("IndexFiles");
            var indexElement = indexFilesElement?.Elements("IndexFile").FirstOrDefault(e => e.Attribute("indexName").Value == indexName);

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
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName);

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
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName);

            if (databaseElement == null)
            {
                throw new ArgumentException($"Database '{databaseName}' does not exist.");
            }

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
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
    }
}
