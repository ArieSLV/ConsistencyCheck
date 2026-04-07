namespace ConsistencyCheck;

internal static class RepairPatchQueryBuilder
{
    public static string BuildTouchWinnerPatchQuery(string collection)
    {
        if (string.IsNullOrWhiteSpace(collection))
            throw new InvalidOperationException("Cannot build a repair patch query without a collection name.");

        return $"from \"{EscapeRqlString(collection)}\" where id() in ($ids) update {{ put(id(this), this); }}";
    }

    private static string EscapeRqlString(string value)
        => value.Replace("\"", "\"\"");
}
