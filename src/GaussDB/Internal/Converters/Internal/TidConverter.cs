using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class TidConverter : PgBufferedConverter<GaussDBTid>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(sizeof(uint) + sizeof(ushort));
        return format is DataFormat.Binary;
    }
    protected override GaussDBTid ReadCore(PgReader reader) => new(reader.ReadUInt32(), reader.ReadUInt16());
    protected override void WriteCore(PgWriter writer, GaussDBTid value)
    {
        writer.WriteUInt32(value.BlockNumber);
        writer.WriteUInt16(value.OffsetNumber);
    }
}
