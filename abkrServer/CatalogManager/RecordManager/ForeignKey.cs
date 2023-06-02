using System.Diagnostics.CodeAnalysis;

namespace abkrServer.CatalogManager.RecordManager
{
    [Serializable]
    public class ForeignKey
    {
        [NotNull] public string TableName { get; set; }
        [NotNull] public string ColumnName { get; set; }
        [NotNull] public string ReferencedTable { get; set; }
        [NotNull] public string ReferencedColumn { get; set; }
    }
}
