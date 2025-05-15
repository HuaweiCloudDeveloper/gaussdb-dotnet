using GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace GaussDB.Internal.Converters;

sealed class LineConverter : PgBufferedConverter<GaussDBLine>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double) * 3);
        return format is DataFormat.Binary;
    }

    protected override GaussDBLine ReadCore(PgReader reader)
        => new(reader.ReadDouble(), reader.ReadDouble(), reader.ReadDouble());

    protected override void WriteCore(PgWriter writer, GaussDBLine value)
    {
        writer.WriteDouble(value.A);
        writer.WriteDouble(value.B);
        writer.WriteDouble(value.C);
    }
}
