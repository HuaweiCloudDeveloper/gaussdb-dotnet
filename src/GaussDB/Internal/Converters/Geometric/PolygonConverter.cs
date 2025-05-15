using System.Threading;
using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class PolygonConverter : PgStreamingConverter<GaussDBPolygon>
{
    public override GaussDBPolygon Read(PgReader reader)
        => Read(async: false, reader, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask<GaussDBPolygon> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => Read(async: true, reader, cancellationToken);

    async ValueTask<GaussDBPolygon> Read(bool async, PgReader reader, CancellationToken cancellationToken)
    {
        if (reader.ShouldBuffer(sizeof(int)))
            await reader.Buffer(async, sizeof(int), cancellationToken).ConfigureAwait(false);
        var numPoints = reader.ReadInt32();
        var result = new GaussDBPolygon(numPoints);
        for (var i = 0; i < numPoints; i++)
        {
            if (reader.ShouldBuffer(sizeof(double) * 2))
                await reader.Buffer(async, sizeof(double) * 2, cancellationToken).ConfigureAwait(false);
            result.Add(new GaussDBPoint(reader.ReadDouble(), reader.ReadDouble()));
        }

        return result;
    }

    public override Size GetSize(SizeContext context, GaussDBPolygon value, ref object? writeState)
        => 4 + value.Count * sizeof(double) * 2;

    public override void Write(PgWriter writer, GaussDBPolygon value)
        => Write(async: false, writer, value, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, GaussDBPolygon value, CancellationToken cancellationToken = default)
        => Write(async: true, writer, value, cancellationToken);

    async ValueTask Write(bool async, PgWriter writer, GaussDBPolygon value, CancellationToken cancellationToken)
    {
        if (writer.ShouldFlush(sizeof(int)))
            await writer.Flush(async, cancellationToken).ConfigureAwait(false);
        writer.WriteInt32(value.Count);

        foreach (var p in value)
        {
            if (writer.ShouldFlush(sizeof(double) * 2))
                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            writer.WriteDouble(p.X);
            writer.WriteDouble(p.Y);
        }
    }
}
