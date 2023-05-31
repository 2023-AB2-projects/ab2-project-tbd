namespace abkr.CatalogManager
{
    public class Index
    {
        public string Name { get; set; }
        public bool IsUnique { get; set; }
        public List<string> Columns { get; set; }

        public Index(string name)
        {
            Name = name;
            Columns = new List<string>();
        }
    }

}