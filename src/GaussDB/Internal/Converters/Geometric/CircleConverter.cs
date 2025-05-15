using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class CircleConverter : PgBufferedConverter<GaussDBCircle>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double) * 3);
        return format is DataFormat.Binary;
    }

    protected override GaussDBCircle ReadCore(PgReader reader)
        => new(reader.ReadDouble(), reader.ReadDouble(), reader.ReadDouble());

    protected override void WriteCore(PgWriter writer, GaussDBCircle value)
    {
        writer.WriteDouble(value.X);
        writer.WriteDouble(value.Y);
        writer.WriteDouble(value.Radius);
    }
}
