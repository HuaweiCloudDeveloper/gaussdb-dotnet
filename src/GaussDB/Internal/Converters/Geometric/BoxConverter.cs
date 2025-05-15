using GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace GaussDB.Internal.Converters;

sealed class BoxConverter : PgBufferedConverter<GaussDBBox>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double) * 4);
        return format is DataFormat.Binary;
    }

    protected override GaussDBBox ReadCore(PgReader reader)
        => new(
            new GaussDBPoint(reader.ReadDouble(), reader.ReadDouble()),
            new GaussDBPoint(reader.ReadDouble(), reader.ReadDouble()));

    protected override void WriteCore(PgWriter writer, GaussDBBox value)
    {
        writer.WriteDouble(value.Right);
        writer.WriteDouble(value.Top);
        writer.WriteDouble(value.Left);
        writer.WriteDouble(value.Bottom);
    }
}
