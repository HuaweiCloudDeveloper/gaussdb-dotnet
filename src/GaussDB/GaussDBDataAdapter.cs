using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// Represents the method that handles the <see cref="GaussDBDataAdapter.RowUpdated"/> events.
/// </summary>
/// <param name="sender">The source of the event.</param>
/// <param name="e">An <see cref="GaussDBRowUpdatedEventArgs"/> that contains the event data.</param>
public delegate void GaussDBRowUpdatedEventHandler(object sender, GaussDBRowUpdatedEventArgs e);

/// <summary>
/// Represents the method that handles the <see cref="GaussDBDataAdapter.RowUpdating"/> events.
/// </summary>
/// <param name="sender">The source of the event.</param>
/// <param name="e">An <see cref="GaussDBRowUpdatingEventArgs"/> that contains the event data.</param>
public delegate void GaussDBRowUpdatingEventHandler(object sender, GaussDBRowUpdatingEventArgs e);

/// <summary>
/// This class represents an adapter from many commands: select, update, insert and delete to fill a <see cref="System.Data.DataSet"/>.
/// </summary>
[System.ComponentModel.DesignerCategory("")]
public sealed class GaussDBDataAdapter : DbDataAdapter
{
    /// <summary>
    /// Row updated event.
    /// </summary>
    public event GaussDBRowUpdatedEventHandler? RowUpdated;

    /// <summary>
    /// Row updating event.
    /// </summary>
    public event GaussDBRowUpdatingEventHandler? RowUpdating;

    /// <summary>
    /// Default constructor.
    /// </summary>
    public GaussDBDataAdapter() {}

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="selectCommand"></param>
    public GaussDBDataAdapter(GaussDBCommand selectCommand)
        => SelectCommand = selectCommand;

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="selectCommandText"></param>
    /// <param name="selectConnection"></param>
    public GaussDBDataAdapter(string selectCommandText, GaussDBConnection selectConnection)
        : this(new GaussDBCommand(selectCommandText, selectConnection)) {}

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="selectCommandText"></param>
    /// <param name="selectConnectionString"></param>
    public GaussDBDataAdapter(string selectCommandText, string selectConnectionString)
        : this(selectCommandText, new GaussDBConnection(selectConnectionString)) {}

    /// <summary>
    /// Create row updated event.
    /// </summary>
    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand? command,
        System.Data.StatementType statementType,
        DataTableMapping tableMapping)
        => new GaussDBRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>
    /// Create row updating event.
    /// </summary>
    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand? command,
        System.Data.StatementType statementType,
        DataTableMapping tableMapping)
        => new GaussDBRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>
    /// Raise the RowUpdated event.
    /// </summary>
    /// <param name="value"></param>
    protected override void OnRowUpdated(RowUpdatedEventArgs value)
    {
        //base.OnRowUpdated(value);
        if (value is GaussDBRowUpdatedEventArgs args)
            RowUpdated?.Invoke(this, args);
        //if (RowUpdated != null && value is GaussDBRowUpdatedEventArgs args)
        //    RowUpdated(this, args);
    }

    /// <summary>
    /// Raise the RowUpdating event.
    /// </summary>
    /// <param name="value"></param>
    protected override void OnRowUpdating(RowUpdatingEventArgs value)
    {
        if (value is GaussDBRowUpdatingEventArgs args)
            RowUpdating?.Invoke(this, args);
    }

    /// <summary>
    /// Delete command.
    /// </summary>
    public new GaussDBCommand? DeleteCommand
    {
        get => (GaussDBCommand?)base.DeleteCommand;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// Select command.
    /// </summary>
    public new GaussDBCommand? SelectCommand
    {
        get => (GaussDBCommand?)base.SelectCommand;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// Update command.
    /// </summary>
    public new GaussDBCommand? UpdateCommand
    {
        get => (GaussDBCommand?)base.UpdateCommand;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// Insert command.
    /// </summary>
    public new GaussDBCommand? InsertCommand
    {
        get => (GaussDBCommand?)base.InsertCommand;
        set => base.InsertCommand = value;
    }

    // Temporary implementation, waiting for official support in System.Data via https://github.com/dotnet/runtime/issues/22109
    [RequiresUnreferencedCode("Members from serialized types or types used in expressions may be trimmed if not referenced directly.")]
    internal async Task<int> Fill(DataTable dataTable, bool async, CancellationToken cancellationToken = default)
    {
        var command = SelectCommand;
        var activeConnection = command?.Connection ?? throw new InvalidOperationException("Connection required");
        var originalState = ConnectionState.Closed;

        try
        {
            originalState = activeConnection.State;
            if (ConnectionState.Closed == originalState)
                await activeConnection.Open(async, cancellationToken).ConfigureAwait(false);

            var dataReader = await command.ExecuteReader(async, CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
            try
            {
                return await Fill(dataTable, dataReader, async, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (async)
                    await dataReader.DisposeAsync().ConfigureAwait(false);
                else
                    dataReader.Dispose();
            }
        }
        finally
        {
            if (ConnectionState.Closed == originalState)
                activeConnection.Close();
        }
    }

    [RequiresUnreferencedCode("Members from serialized types or types used in expressions may be trimmed if not referenced directly.")]
    async Task<int> Fill(DataTable dataTable, GaussDBDataReader dataReader, bool async, CancellationToken cancellationToken = default)
    {
        dataTable.BeginLoadData();
        try
        {
            var rowsAdded = 0;
            var count = dataReader.FieldCount;
            var columnCollection = dataTable.Columns;
            for (var i = 0; i < count; ++i)
            {
                var fieldName = dataReader.GetName(i);
                if (!columnCollection.Contains(fieldName))
                {
                    var fieldType = dataReader.GetFieldType(i);
                    var dataColumn = new DataColumn(fieldName, fieldType);
                    columnCollection.Add(dataColumn);
                }
            }

            var values = new object[count];

            while (async ? await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false) : dataReader.Read())
            {
                dataReader.GetValues(values);
                dataTable.LoadDataRow(values, true);
                rowsAdded++;
            }
            return rowsAdded;
        }
        finally
        {
            dataTable.EndLoadData();
        }
    }
}

#pragma warning disable 1591

public class GaussDBRowUpdatingEventArgs(
    DataRow dataRow,
    IDbCommand? command,
    System.Data.StatementType statementType,
    DataTableMapping tableMapping)
    : RowUpdatingEventArgs(dataRow, command, statementType, tableMapping);

public class GaussDBRowUpdatedEventArgs(
    DataRow dataRow,
    IDbCommand? command,
    System.Data.StatementType statementType,
    DataTableMapping tableMapping)
    : RowUpdatedEventArgs(dataRow, command, statementType, tableMapping);

#pragma warning restore 1591
