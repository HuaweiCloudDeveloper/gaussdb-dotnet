using System.Diagnostics.CodeAnalysis;

namespace GaussDB.Internal.Postgres;

/// Base field type shared between tables and composites.
[Experimental(GaussDBDiagnostics.ConvertersExperimental)]
public readonly struct Field(string name, PgTypeId pgTypeId, int typeModifier)
{
    public string Name { get; init; } = name;
    public PgTypeId PgTypeId { get; init; } = pgTypeId;
    public int TypeModifier { get; init; } = typeModifier;
}
