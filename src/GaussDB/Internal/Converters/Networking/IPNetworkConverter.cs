using System.Net;

// ReSharper disable once CheckNamespace
namespace GaussDB.Internal.Converters;

sealed class IPNetworkConverter : PgBufferedConverter<IPNetwork>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
        => CanConvertBufferedDefault(format, out bufferRequirements);

    public override Size GetSize(SizeContext context, IPNetwork value, ref object? writeState)
        => GaussDBInetConverter.GetSizeImpl(context, value.BaseAddress, ref writeState);

    protected override IPNetwork ReadCore(PgReader reader)
    {
        var (ip, netmask) = GaussDBInetConverter.ReadImpl(reader, shouldBeCidr: true);
        return new(ip, netmask);
    }

    protected override void WriteCore(PgWriter writer, IPNetwork value)
        => GaussDBInetConverter.WriteImpl(writer, (value.BaseAddress, (byte)value.PrefixLength), isCidr: true);
}
