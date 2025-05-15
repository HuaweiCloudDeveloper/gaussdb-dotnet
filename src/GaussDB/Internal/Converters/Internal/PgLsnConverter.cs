using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class PgLsnConverter : PgBufferedConverter<GaussDBLogSequenceNumber>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(ulong));
        return format is DataFormat.Binary;
    }
    protected override GaussDBLogSequenceNumber ReadCore(PgReader reader) => new(reader.ReadUInt64());
    protected override void WriteCore(PgWriter writer, GaussDBLogSequenceNumber value) => writer.WriteUInt64((ulong)value);
}
