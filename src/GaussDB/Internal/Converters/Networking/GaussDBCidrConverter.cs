using HuaweiCloud.GaussDBTypes;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

#pragma warning disable CS0618 // GaussDBCidr is obsolete
sealed class GaussDBCidrConverter : PgBufferedConverter<GaussDBCidr>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
        => GaussDBInetConverter.CanConvertImpl(format, out bufferRequirements);

    public override Size GetSize(SizeContext context, GaussDBCidr value, ref object? writeState)
        => GaussDBInetConverter.GetSizeImpl(context, value.Address, ref writeState);

    protected override GaussDBCidr ReadCore(PgReader reader)
    {
        var (ip, netmask) = GaussDBInetConverter.ReadImpl(reader, shouldBeCidr: true);
        return new(ip, netmask);
    }

    protected override void WriteCore(PgWriter writer, GaussDBCidr value)
        => GaussDBInetConverter.WriteImpl(writer, (value.Address, value.Netmask), isCidr: true);
}
#pragma warning restore CS0618
