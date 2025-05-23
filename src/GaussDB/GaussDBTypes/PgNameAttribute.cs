using System;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDBTypes;

/// <summary>
/// Indicates that this property or field corresponds to a PostgreSQL field with the specified name
/// </summary>
[AttributeUsage(
    AttributeTargets.Enum |
    AttributeTargets.Class |
    AttributeTargets.Struct |
    AttributeTargets.Field |
    AttributeTargets.Property |
    AttributeTargets.Parameter)]
public class PgNameAttribute : Attribute
{
    /// <summary>
    /// The name of PostgreSQL field that corresponds to this CLR property or field
    /// </summary>
    public string PgName { get; }

    /// <summary>
    /// Indicates that this property or field corresponds to a PostgreSQL field with the specified name
    /// </summary>
    /// <param name="pgName">The name of PostgreSQL field that corresponds to this CLR property or field</param>
    public PgNameAttribute(string pgName)
        => PgName = pgName;
}
