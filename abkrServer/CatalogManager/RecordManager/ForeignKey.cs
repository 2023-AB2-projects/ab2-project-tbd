public class ForeignKey : IEquatable<ForeignKey>
{
    public string TableName { get; }
    public string ColumnName { get; }
    public string ReferencedTable { get; }
    public string ReferencedColumn { get; }
    public bool IsUnique { get; }

    public ForeignKey(string tableName, string columnName, string referencedTable, string referencedColumn, bool isUnique)
    {
        TableName = tableName;
        ColumnName = columnName;
        ReferencedTable = referencedTable;
        ReferencedColumn = referencedColumn;
        IsUnique = isUnique;
    }

    public bool Equals(ForeignKey? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return TableName == other.TableName &&
               ColumnName == other.ColumnName &&
               ReferencedTable == other.ReferencedTable &&
               ReferencedColumn == other.ReferencedColumn &&
               IsUnique == other.IsUnique;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as ForeignKey);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(TableName, ColumnName, ReferencedTable, ReferencedColumn, IsUnique);
    }
}