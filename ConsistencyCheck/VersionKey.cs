using System.Security.Cryptography;
using System.Text;

namespace ConsistencyCheck;

/// <summary>
/// Exact dedup identity for one processed document version.
/// </summary>
public readonly record struct VersionKey(
    string Id,
    string NormalizedChangeVector,
    string HashHex,
    string Prefix)
{
    public static VersionKey Create(string id, string normalizedChangeVector)
    {
        var payload = Encoding.UTF8.GetBytes($"{id}\n{normalizedChangeVector}");
        var hash = SHA256.HashData(payload);
        var hashHex = Convert.ToHexString(hash);
        return new VersionKey(id, normalizedChangeVector, hashHex, hashHex[..4]);
    }
}
