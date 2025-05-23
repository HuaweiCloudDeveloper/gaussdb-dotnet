using System;
using HuaweiCloud.GaussDB.Util;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// Large object manager. This class can be used to store very large files in a PostgreSQL database.
/// </summary>
[Obsolete("GaussDBLargeObjectManager allows manipulating PostgreSQL large objects via publicly available PostgreSQL functions (lo_read, lo_write); call these yourself directly.")]
public class GaussDBLargeObjectManager
{
    const int InvWrite = 0x00020000;
    const int InvRead = 0x00040000;

    internal GaussDBConnection Connection { get; }

    /// <summary>
    /// The largest chunk size (in bytes) read and write operations will read/write each roundtrip to the network. Default 4 MB.
    /// </summary>
    public int MaxTransferBlockSize { get; set; }

    /// <summary>
    /// Creates an GaussDBLargeObjectManager for this connection. The connection must be opened to perform remote operations.
    /// </summary>
    /// <param name="connection"></param>
    public GaussDBLargeObjectManager(GaussDBConnection connection)
    {
        Connection = connection;
        MaxTransferBlockSize = 4 * 1024 * 1024; // 4MB
    }

    /// <summary>
    /// Execute a function
    /// </summary>
    internal async Task<T> ExecuteFunction<T>(bool async, string function, CancellationToken cancellationToken, params object[] arguments)
    {
        using var command = Connection.CreateCommand();
        var stringBuilder = new StringBuilder("SELECT * FROM ").Append(function).Append('(');

        for (var i = 0; i < arguments.Length; i++)
        {
            if (i > 0)
                stringBuilder.Append(", ");
            stringBuilder.Append('$').Append(i + 1);
            command.Parameters.Add(new GaussDBParameter { Value = arguments[i] });
        }

        stringBuilder.Append(')');
        command.CommandText = stringBuilder.ToString();

        return (T)(async ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) : command.ExecuteScalar())!;
    }

    /// <summary>
    /// Execute a function that returns a byte array
    /// </summary>
    /// <returns></returns>
    internal async Task<int> ExecuteFunctionGetBytes(
        bool async, string function, byte[] buffer, int offset, int len, CancellationToken cancellationToken, params object[] arguments)
    {
        using var command = Connection.CreateCommand();
        var stringBuilder = new StringBuilder("SELECT * FROM ").Append(function).Append('(');

        for (var i = 0; i < arguments.Length; i++)
        {
            if (i > 0)
                stringBuilder.Append(", ");
            stringBuilder.Append('$').Append(i + 1);
            command.Parameters.Add(new GaussDBParameter { Value = arguments[i] });
        }

        stringBuilder.Append(')');
        command.CommandText = stringBuilder.ToString();

        var reader = async
            ? await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false)
            : command.ExecuteReader(CommandBehavior.SequentialAccess);
        try
        {
            if (async)
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            else
                reader.Read();

            return (int)reader.GetBytes(0, 0, buffer, offset, len);
        }
        finally
        {
            if (async)
                await reader.DisposeAsync().ConfigureAwait(false);
            else
                reader.Dispose();
        }
    }

    /// <summary>
    /// Create an empty large object in the database. If an oid is specified but is already in use, an PostgresException will be thrown.
    /// </summary>
    /// <param name="preferredOid">A preferred oid, or specify 0 if one should be automatically assigned</param>
    /// <returns>The oid for the large object created</returns>
    /// <exception cref="PostgresException">If an oid is already in use</exception>
    public uint Create(uint preferredOid = 0) => Create(preferredOid, false).GetAwaiter().GetResult();

    // Review unused parameters
    /// <summary>
    /// Create an empty large object in the database. If an oid is specified but is already in use, an PostgresException will be thrown.
    /// </summary>
    /// <param name="preferredOid">A preferred oid, or specify 0 if one should be automatically assigned</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    /// <returns>The oid for the large object created</returns>
    /// <exception cref="PostgresException">If an oid is already in use</exception>
    public Task<uint> CreateAsync(uint preferredOid, CancellationToken cancellationToken = default)
        => Create(preferredOid, true, cancellationToken);

    Task<uint> Create(uint preferredOid, bool async, CancellationToken cancellationToken = default)
        => ExecuteFunction<uint>(async, "lo_create", cancellationToken, (int)preferredOid);

    /// <summary>
    /// Opens a large object on the backend, returning a stream controlling this remote object.
    /// A transaction snapshot is taken by the backend when the object is opened with only read permissions.
    /// When reading from this object, the contents reflects the time when the snapshot was taken.
    /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
    /// </summary>
    /// <param name="oid">Oid of the object</param>
    /// <returns>An GaussDBLargeObjectStream</returns>
    public GaussDBLargeObjectStream OpenRead(uint oid)
        => OpenRead(async: false, oid).GetAwaiter().GetResult();

    /// <summary>
    /// Opens a large object on the backend, returning a stream controlling this remote object.
    /// A transaction snapshot is taken by the backend when the object is opened with only read permissions.
    /// When reading from this object, the contents reflects the time when the snapshot was taken.
    /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
    /// </summary>
    /// <param name="oid">Oid of the object</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    /// <returns>An GaussDBLargeObjectStream</returns>
    public Task<GaussDBLargeObjectStream> OpenReadAsync(uint oid, CancellationToken cancellationToken = default)
        => OpenRead(async: true, oid, cancellationToken);

    async Task<GaussDBLargeObjectStream> OpenRead(bool async, uint oid, CancellationToken cancellationToken = default)
    {
        var fd = await ExecuteFunction<int>(async, "lo_open", cancellationToken, (int)oid, InvRead).ConfigureAwait(false);
        return new GaussDBLargeObjectStream(this, fd, false);
    }

    /// <summary>
    /// Opens a large object on the backend, returning a stream controlling this remote object.
    /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
    /// </summary>
    /// <param name="oid">Oid of the object</param>
    /// <returns>An GaussDBLargeObjectStream</returns>
    public GaussDBLargeObjectStream OpenReadWrite(uint oid)
        => OpenReadWrite(async: false, oid).GetAwaiter().GetResult();

    /// <summary>
    /// Opens a large object on the backend, returning a stream controlling this remote object.
    /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
    /// </summary>
    /// <param name="oid">Oid of the object</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    /// <returns>An GaussDBLargeObjectStream</returns>
    public Task<GaussDBLargeObjectStream> OpenReadWriteAsync(uint oid, CancellationToken cancellationToken = default)
        => OpenReadWrite(async: true, oid, cancellationToken);

    async Task<GaussDBLargeObjectStream> OpenReadWrite(bool async, uint oid, CancellationToken cancellationToken = default)
    {
        var fd = await ExecuteFunction<int>(async, "lo_open", cancellationToken, (int)oid, InvRead | InvWrite).ConfigureAwait(false);
        return new GaussDBLargeObjectStream(this, fd, true);
    }

    /// <summary>
    /// Deletes a large object on the backend.
    /// </summary>
    /// <param name="oid">Oid of the object to delete</param>
    public void Unlink(uint oid)
        => ExecuteFunction<object>(async: false, "lo_unlink", CancellationToken.None, (int)oid).GetAwaiter().GetResult();

    /// <summary>
    /// Deletes a large object on the backend.
    /// </summary>
    /// <param name="oid">Oid of the object to delete</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    public Task UnlinkAsync(uint oid, CancellationToken cancellationToken = default)
        => ExecuteFunction<object>(async: true, "lo_unlink", cancellationToken, (int)oid);

    /// <summary>
    /// Exports a large object stored in the database to a file on the backend. This requires superuser permissions.
    /// </summary>
    /// <param name="oid">Oid of the object to export</param>
    /// <param name="path">Path to write the file on the backend</param>
    public void ExportRemote(uint oid, string path)
        => ExecuteFunction<object>(async: false, "lo_export", CancellationToken.None, (int)oid, path).GetAwaiter().GetResult();

    /// <summary>
    /// Exports a large object stored in the database to a file on the backend. This requires superuser permissions.
    /// </summary>
    /// <param name="oid">Oid of the object to export</param>
    /// <param name="path">Path to write the file on the backend</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    public Task ExportRemoteAsync(uint oid, string path, CancellationToken cancellationToken = default)
        => ExecuteFunction<object>(async: true, "lo_export", cancellationToken, (int)oid, path);

    /// <summary>
    /// Imports a large object to be stored as a large object in the database from a file stored on the backend. This requires superuser permissions.
    /// </summary>
    /// <param name="path">Path to read the file on the backend</param>
    /// <param name="oid">A preferred oid, or specify 0 if one should be automatically assigned</param>
    public void ImportRemote(string path, uint oid = 0)
        => ExecuteFunction<object>(async: false, "lo_import", CancellationToken.None, path, (int)oid).GetAwaiter().GetResult();

    /// <summary>
    /// Imports a large object to be stored as a large object in the database from a file stored on the backend. This requires superuser permissions.
    /// </summary>
    /// <param name="path">Path to read the file on the backend</param>
    /// <param name="oid">A preferred oid, or specify 0 if one should be automatically assigned</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    public Task ImportRemoteAsync(string path, uint oid, CancellationToken cancellationToken = default)
        => ExecuteFunction<object>(async: true, "lo_import", cancellationToken, path, (int)oid);

    /// <summary>
    /// Since PostgreSQL 9.3, large objects larger than 2GB can be handled, up to 4TB.
    /// This property returns true whether the PostgreSQL version is >= 9.3.
    /// </summary>
    public bool Has64BitSupport => Connection.PostgreSqlVersion.IsGreaterOrEqual(9, 3);
}
