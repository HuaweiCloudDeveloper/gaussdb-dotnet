using System;
using System.Collections.Generic;

namespace GaussDB;

/// <summary>
/// Options for configuring GaussDB type loading.
/// </summary>
sealed class GaussDBTypeLoadingOptions
{
    /// <summary>
    /// Load table composite type definitions, and not just free-standing composite types.
    /// </summary>
    public required bool LoadTableComposites { get; init; }

    /// <summary>
    /// When false, if the server doesn't support full type loading from the PostgreSQL catalogs,
    /// support the basic set of types via information hardcoded inside GaussDB.
    /// </summary>
    public required bool LoadTypes { get; init; } = true;

    /// <summary>
    /// Load type definitions from the given schemas.
    /// </summary>
    public required string[]? TypeLoadingSchemas { get; init; }
}

/// <summary>
/// Options builder for configuring GaussDB type loading.
/// </summary>
public sealed class GaussDBTypeLoadingOptionsBuilder
{
    bool _loadTableComposites;
    bool _loadTypes = true;
    List<string>? _typeLoadingSchemas;

    internal GaussDBTypeLoadingOptionsBuilder() {}

    /// <summary>
    /// Enable loading table composite type definitions, and not just free-standing composite types.
    /// </summary>
    public GaussDBTypeLoadingOptionsBuilder EnableTableCompositesLoading(bool enable = true)
    {
        _loadTableComposites = enable;
        return this;
    }

    /// <summary>
    /// Enable loading of types, when disabled GaussDB falls back to a small, builtin, set of known types and type ids.
    /// </summary>
    public GaussDBTypeLoadingOptionsBuilder EnableTypeLoading(bool enable = true)
    {
        _loadTypes = enable;
        return this;
    }

    /// <summary>
    /// Set the schemas to load types from, this can be used to reduce the work done during type loading.
    /// </summary>
    /// <remarks>GaussDB will always load types from the following schemas: pg_catalog, information_schema, pg_toast.
    /// Any user-defined types (typcategory 'U') will also be loaded regardless of their schema.</remarks>
    /// <param name="schemas">Schemas to load types from.</param>
    public GaussDBTypeLoadingOptionsBuilder SetTypeLoadingSchemas(params IEnumerable<string>? schemas)
    {
        if (schemas is null)
        {
            _typeLoadingSchemas = null;
            return this;
        }

        _typeLoadingSchemas = new();
        foreach (var schema in schemas)
        {
            if (schema is not { Length: > 0 })
            {
                _typeLoadingSchemas = null;
                throw new ArgumentException("Schema cannot be null or empty.");
            }
            _typeLoadingSchemas.Add(schema);
        }

        return this;
    }

    internal GaussDBTypeLoadingOptions Build() => new()
    {
        LoadTableComposites = _loadTableComposites,
        LoadTypes = _loadTypes,
        TypeLoadingSchemas = _typeLoadingSchemas?.ToArray()
    };
}

/// <summary>
/// An option specified in the connection string that activates special compatibility features.
/// </summary>
public enum ServerCompatibilityMode
{
    /// <summary>
    /// No special server compatibility mode is active
    /// </summary>
    None,

    /// <summary>
    /// The server is an Amazon Redshift instance.
    /// </summary>
    [Obsolete("ServerCompatibilityMode.Redshift no longer does anything and can be safely removed.")]
    Redshift,

    /// <summary>
    /// The server is doesn't support full type loading from the PostgreSQL catalogs, support the basic set
    /// of types via information hardcoded inside GaussDB.
    /// </summary>
    NoTypeLoading,
}
