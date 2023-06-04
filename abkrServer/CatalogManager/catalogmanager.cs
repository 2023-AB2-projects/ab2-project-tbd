using System.Xml.Linq;
using abkr.ServerLogger;
using abkrServer.CatalogManager.RecordManager;

namespace abkr.CatalogManager
{
    public class CatalogManager
    {
        private readonly string _metadataFilePath;
        private Logger logger;

        public CatalogManager(string metadataFilePath, Logger l)
        {
            logger = l;
            _metadataFilePath = metadataFilePath;
        }

        public XElement LoadMetadata()
        {
            if (!File.Exists(_metadataFilePath))
            {
                logger.LogMessage($"Metadata file not found at {_metadataFilePath}. Creating a new file.");
                var databasesElement = new XElement("Databases");
                databasesElement.Save(_metadataFilePath);
            }

            try
            {
                return XElement.Load(_metadataFilePath);
            }
            catch (Exception ex)
            {
                logger.LogMessage($"Error reading metadata file: {ex.Message}");
                return new XElement("Databases");
            }
        }

        public void SaveMetadata(XElement metadata)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_metadataFilePath));
            metadata.Save(_metadataFilePath);
            logger.LogMessage($"Metadata saved to {_metadataFilePath}");
        }

        public List<Index> GetIndexes(string databaseName, string tableName)
        {
            var indexes = new List<Index>();
            var metadata = LoadMetadata();
            var tableElement = GetTableElement(databaseName, tableName, metadata);

            var indexFilesElement = tableElement.Element("IndexFiles");
            if (indexFilesElement != null)
            {
                foreach (var indexElement in indexFilesElement.Elements("IndexFile"))
                {
                    var indexName = indexElement.Attribute("indexName")?.Value;
                    var isUnique = bool.Parse(indexElement.Attribute("isUnique")?.Value ?? "false");

                    var index = new Index(indexName, isUnique);

                    var indexAttributesElement = indexElement.Element("IndexAttributes");
                    if (indexAttributesElement != null)
                    {
                        index.Columns.AddRange(indexAttributesElement.Elements("IAttribute").Select(a => a.Value));
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

            var tableElement = GetTableElement(databaseName, tableName, metadata);

            var structureElement = tableElement.Element("Structure");
            if (structureElement != null)
            {
                foreach (var attributeElement in structureElement.Elements("Attribute"))
                {
                    var isForeignKey = attributeElement.Attribute("isForeignKey")?.Value == "true";
                    if (isForeignKey)
                    {
                        var attributeName = attributeElement.Attribute("attributeName")?.Value;

                        logger.LogMessage($"Found foreign key {attributeName} in table {tableName}");

                        var references = attributeElement.Attribute("references")?.Value;

                        logger.LogMessage($"References: {references}");

                        var isUnique = bool.Parse(attributeElement.Attribute("isUnique")?.Value ?? "false");

                        logger.LogMessage($"Is unique: {isUnique}");

                        if (!string.IsNullOrEmpty(attributeName) && !string.IsNullOrEmpty(references))
                        {
                            // Assumes the references string is in format "referencedTable:referencedColumn"
                            var splitReferences = references.Split('(');
                            if (splitReferences.Length == 2)
                            {
                                var referencedTable = splitReferences[0].Split('.')[1];
                                var referencedColumn = splitReferences[1].Split(')')[0];
                                var pk = GetPrimaryKeyColumn(databaseName, referencedTable);
                                bool ispk = false;
                                if(pk == referencedColumn)
                                {
                                    ispk= true;
                                }
                                else
                                {
                                    ispk = false;
                                }
                                (string, bool) t = (referencedColumn, ispk);
                                
                                var foreignKey = new ForeignKey(tableName, attributeName, referencedTable, t , isUnique);
                                foreignKeys.Add(foreignKey);
                            }
                        }
                    }
                }
            }

            return foreignKeys;
        }

        private static XElement GetTableElement(string databaseName, string tableName, XElement metadata)
        {
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName")?.Value == tableName)
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            return tableElement;
        }



        public void CreateDatabase(string databaseName)
        {
            var metadata = LoadMetadata();
            var databaseElement = new XElement("DataBase", new XAttribute("dataBaseName", databaseName));
            metadata.Add(databaseElement);
            SaveMetadata(metadata);
            logger.LogMessage($"Database '{databaseName}' created.");
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

        public int? GetColumnLineNumber(string databaseName, string tableName, string columnName)
        {
            // Load the existing metadata
            var metadata = LoadMetadata();

            // Ensure the database exists
            var databaseElement = metadata.Elements("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName")?.Value == databaseName)
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            // Ensure the table exists
            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName)
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            // Get the structure element
            var structureElement = tableElement.Element("Structure");

            // If the structure doesn't exist or there are no attributes, return null
            if (structureElement == null || !structureElement.HasElements)
            {
                return null;
            }

            // Get the list of all attribute elements
            var attributes = structureElement.Elements("Attribute").ToList();

            // Find the attribute with the given column name and get its line number
            for (int i = 0; i < attributes.Count; i++)
            {
                if (attributes[i].Attribute("attributeName")?.Value == columnName)
                {
                    return i + 1;  // return line number starting from 1
                }
            }

            // If the column was not found, return null
            return null;
        }



        public void CreateTable(
            string databaseName,
            string tableName,
            List<Column> columns)
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

            logger.LogMessage($"CatalogManagar.CreateTable: Creating table '{tableName}' in database '{databaseName}'.");

            // Check if the table already exists
            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName);
            if (tableElement != null)
            {
                throw new ArgumentException($"Table '{tableName}' already exists in database '{databaseName}'.");
            }

            // If the table doesn't exist, create a new one
            tableElement = new XElement("Table", new XAttribute("tableName", tableName));


            // Create a new structure element
            var structureElement = new XElement("Structure");

            // Iterate through the provided columns
            foreach (var column in columns)
            {
                //logger.LogMessage($"CatalogManager.CreateTable: Adding column '{column.Name}' to table '{tableName}' in iteration {i++}.");
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
                    logger.LogMessage($"Primary key attribute added for column: {column.Name}");
                    logger.LogMessage($"Unique key attribute added for primary key: {column.Name}");
                }

                // Check if the column is a foreign key
                if (column.ForeignKeyReference != null)
                {
                    // Split the ForeignKeyReference to get the table and column 

                    string foreignTable = column.ForeignKeyReference.ReferencedTable;
                    string foreignColumn = column.ForeignKeyReference.ReferencedColumn.Item1;

                    logger.LogMessage($"Foreign key attribute added from column: {foreignColumn} in Table: {foreignTable}");

                    // Check if the referenced column in the foreign table is unique
                    if (!IsForeignKeyUnique(databaseName, foreignTable, foreignColumn))
                    {

                        //throw new ArgumentException($"ForeignKeyReference for column '{column.Name}' references a column '{foreignColumn}' " +
                        //    $"in table '{foreignTable}' with a cotradiction in UNIQUE CONSTRAINT.", nameof(columns));
                        column.IsUnique = false;
                    }
                    else
                    {
                        column.IsUnique= true;
                    }

                    // Check if the types are consistent
                    CheckTypeConsistency(databaseName, foreignTable, foreignColumn, column.Type.ToString());


                    attribute.SetAttributeValue("isForeignKey", "true");
                    attribute.SetAttributeValue("references", $"{databaseName}.{foreignTable}({foreignColumn})");
                    logger.LogMessage($"Foreign key attribute added for column: {column.Name}");
                }

                // Check if the column is a unique key
                if (column.IsUnique)
                {
                    attribute.SetAttributeValue("isUnique", "true");
                    logger.LogMessage($"Unique key attribute added for column: {column.Name}");
                }

                structureElement.Add(attribute);
            }

            logger.LogMessage("CatalogManager.CreateTable: Adding primary key element.");

            if (columns.Where(c=>c.IsPrimaryKey)==null)
            {
                throw new ArgumentNullException("Primary key column cannot be null or empty.");
            }

            var primaryKeyElement = new XElement("primaryKey", new XAttribute("name", columns.Where(c => c.IsPrimaryKey).FirstOrDefault().Name));
            tableElement.Add(structureElement);
            tableElement.Add(primaryKeyElement); // Add primary key element to table
            databaseElement.Add(tableElement);

            logger.LogMessage("CatalogManager.CreateTable: Saving metadata.");

            SaveMetadata(metadata);
        }

        public List<string> GetColumnNames(string databaseName, string tableName)
        {
            // Load the existing metadata
            var metadata = LoadMetadata();

            // Select the database
            var databaseElement = metadata.Descendants("DataBase").FirstOrDefault(e => e.Attribute("dataBaseName").Value == databaseName) 
                ?? throw new ArgumentException($"Database '{databaseName}' does not exist.");

            // Select the table
            var tableElement = databaseElement.Descendants("Table").FirstOrDefault(e => e.Attribute("tableName").Value == tableName) 
                ?? throw new ArgumentException($"Table '{tableName}' does not exist in database '{databaseName}'.");

            // Get the column names
            var columnNames = tableElement.Descendants("Attribute").Select(a => a.Attribute("attributeName").Value).ToList();

            return columnNames;
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

        public bool IsForeignKeyUnique(string databaseName, string foreignTable, string foreignColumn)
        {
            XElement metadata = LoadMetadata();
            XElement? databaseElement = metadata?.Elements("DataBase").FirstOrDefault(db => db.Attribute("dataBaseName")?.Value == databaseName)
                ??throw new Exception($"CatalogManager.IsForeignKeyUnique: Database {databaseName} not found.");

            XElement? foreignTableElement = (databaseElement?.Elements("Table").FirstOrDefault(tbl => tbl.Attribute("tableName")?.Value == foreignTable)
                ??throw new Exception($"CatalogManager.IsForeignKeyUnique: Table {foreignTable} not found in database {databaseName}")) ?? throw new Exception($"ERROR: Table {foreignTable} not found in database {databaseName}!");
            
            XElement? foreignColumnAttribute = foreignTableElement.Elements("Structure").Elements("Attribute")
                .FirstOrDefault(attr => attr.Attribute("attributeName")?.Value == foreignColumn) 
                ?? throw new Exception($"CatalogManager.IsForeignKeyUnique: Column {foreignColumn} not found in table {foreignTable}!");
            var isUniqueAttribute = foreignColumnAttribute.Attribute("isUnique")?.Value;

            return isUniqueAttribute == "true";
        }

        public void CheckTypeConsistency(string databaseName, string foreignTable, string foreignColumn, string type)
        {
            XElement metadata = LoadMetadata();

            XElement? databaseElement = metadata?.Elements("DataBase").FirstOrDefault(db => db.Attribute("dataBaseName")?.Value == databaseName)
                ??throw new Exception($"CatalogManager.CheckTypeConsistency: Database {databaseName} not found.");

            XElement? foreignTableElement = databaseElement?.Elements("Table").FirstOrDefault(tbl => tbl.Attribute("tableName")?.Value == foreignTable)
                ??throw new Exception($"CatalogManager.CheckTypeConsistency: Table {foreignTable} not found in {databaseName}");

            XElement? foreignColumnAttribute = foreignTableElement.Elements("Structure").Elements("Attribute")
                .FirstOrDefault(attr => attr.Attribute("attributeName")?.Value == foreignColumn) 
                ?? throw new Exception($"CatalogManager.CheckTypeConsistency: Column {foreignColumn} not found in table {foreignTable}!");

            var foreignColumnType = foreignColumnAttribute.Attribute("type")?.Value;
            if (foreignColumnType != type)
            {
                throw new Exception($"CatalogManager.CheckTypeConsistency: Type mismatch. Expected type for column {foreignColumn} in table {foreignTable} is {foreignColumnType} but received {type}!");
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
