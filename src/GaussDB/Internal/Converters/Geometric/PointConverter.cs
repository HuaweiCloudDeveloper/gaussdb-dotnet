using GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace GaussDB.Internal.Converters;

sealed class PointConverter : PgBufferedConverter<GaussDBPoint>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double) * 2);
        return format is DataFormat.Binary;
    }

    protected override GaussDBPoint ReadCore(PgReader reader)
        => new(reader.ReadDouble(), reader.ReadDouble());

    protected override void WriteCore(PgWriter writer, GaussDBPoint value)
    {
        writer.WriteDouble(value.X);
        writer.WriteDouble(value.Y);
    }
}
