using System;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using GaussDB.Internal;
using GaussDBTypes;

namespace GaussDB;

/// <summary>
/// A generic version of <see cref="GaussDBParameter"/> which provides more type safety and
/// avoids boxing of value types. Use <see cref="TypedValue"/> instead of <see cref="GaussDBParameter.Value"/>.
/// </summary>
/// <typeparam name="T">The type of the value that will be stored in the parameter.</typeparam>
public sealed class GaussDBParameter<T> : GaussDBParameter
{
    T? _typedValue;

    /// <summary>
    /// Gets or sets the strongly-typed value of the parameter.
    /// </summary>
    public T? TypedValue
    {
        get => _typedValue;
        set
        {
            if (typeof(T) == typeof(object) && ShouldResetObjectTypeInfo(value))
                ResetTypeInfo();
            else
                ResetBindingInfo();
            _typedValue = value;
        }
    }

    /// <summary>
    /// Gets or sets the value of the parameter. This delegates to <see cref="TypedValue"/>.
    /// </summary>
    public override object? Value
    {
        get => TypedValue;
        set => TypedValue = (T)value!;
    }

    private protected override Type StaticValueType => typeof(T);

    #region Constructors

    /// <summary>
    /// Initializes a new instance of <see cref="GaussDBParameter{T}" />.
    /// </summary>
    public GaussDBParameter() { }

    /// <summary>
    /// Initializes a new instance of <see cref="GaussDBParameter{T}" /> with a parameter name and value.
    /// </summary>
    public GaussDBParameter(string parameterName, T value)
    {
        ParameterName = parameterName;
        TypedValue = value;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="GaussDBParameter{T}" /> with a parameter name and type.
    /// </summary>
    public GaussDBParameter(string parameterName, GaussDBDbType gaussdbDbType)
    {
        ParameterName = parameterName;
        GaussDBDbType = gaussdbDbType;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="GaussDBParameter{T}" /> with a parameter name and type.
    /// </summary>
    public GaussDBParameter(string parameterName, DbType dbType)
    {
        ParameterName = parameterName;
        DbType = dbType;
    }

    #endregion Constructors

    private protected override PgConverterResolution ResolveConverter(PgTypeInfo typeInfo)
    {
        if (typeof(T) == typeof(object) || TypeInfo!.IsBoxing)
            return base.ResolveConverter(typeInfo);

        _asObject = false;
        return typeInfo.GetResolution(TypedValue);
    }

    // We ignore allowNullReference, it's just there to control the base implementation.
    private protected override void BindCore(DataFormat? formatPreference, bool allowNullReference = false)
    {
        if (_asObject)
        {
            // If we're object typed we should not support null.
            base.BindCore(formatPreference, typeof(T) != typeof(object));
            return;
        }

        var value = TypedValue;
        if (TypeInfo!.Bind(Converter!.UnsafeDowncast<T>(), value, out var size, out _writeState, out var dataFormat, formatPreference) is { } info)
        {
            WriteSize = size;
            _bufferRequirement = info.BufferRequirement;
        }
        else
        {
            WriteSize = -1;
            _bufferRequirement = default;
        }

        Format = dataFormat;
    }

    private protected override ValueTask WriteValue(bool async, PgWriter writer, CancellationToken cancellationToken)
    {
        if (_asObject)
            return base.WriteValue(async, writer, cancellationToken);

        if (async)
            return Converter!.UnsafeDowncast<T>().WriteAsync(writer, TypedValue!, cancellationToken);

        Converter!.UnsafeDowncast<T>().Write(writer, TypedValue!);
        return new();
    }

    private protected override GaussDBParameter CloneCore() =>
        // use fields instead of properties
        // to avoid auto-initializing something like type_info
        new GaussDBParameter<T>
        {
            _precision = _precision,
            _scale = _scale,
            _size = _size,
            _gaussdbDbType = _gaussdbDbType,
            _dataTypeName = _dataTypeName,
            Direction = Direction,
            IsNullable = IsNullable,
            _name = _name,
            TrimmedName = TrimmedName,
            SourceColumn = SourceColumn,
            SourceVersion = SourceVersion,
            TypedValue = TypedValue,
            SourceColumnNullMapping = SourceColumnNullMapping,
        };
}
