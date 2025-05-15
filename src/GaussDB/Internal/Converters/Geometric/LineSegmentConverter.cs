using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class LineSegmentConverter : PgBufferedConverter<GaussDBLSeg>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double) * 4);
        return format is DataFormat.Binary;
    }

    protected override GaussDBLSeg ReadCore(PgReader reader)
        => new(reader.ReadDouble(), reader.ReadDouble(), reader.ReadDouble(), reader.ReadDouble());

    protected override void WriteCore(PgWriter writer, GaussDBLSeg value)
    {
        writer.WriteDouble(value.Start.X);
        writer.WriteDouble(value.Start.Y);
        writer.WriteDouble(value.End.X);
        writer.WriteDouble(value.End.Y);
    }
}
