namespace abkr.CatalogManager
{ 
    public class Index
    {
        public string Name { get; }
        public bool IsUnique { get; set; }
        public List<string> Columns { get; } = new();

        public Index(string name, bool isUnique)
        {
            Name = name;
            IsUnique = isUnique;
        }
    }
}
