using System.Net;
using System.Net.Sockets;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class IPAddressConverter : PgBufferedConverter<IPAddress>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
        => GaussDBInetConverter.CanConvertImpl(format, out bufferRequirements);

    public override Size GetSize(SizeContext context, IPAddress value, ref object? writeState)
        => GaussDBInetConverter.GetSizeImpl(context, value, ref writeState);

    protected override IPAddress ReadCore(PgReader reader)
        => GaussDBInetConverter.ReadImpl(reader, shouldBeCidr: false).Address;

    protected override void WriteCore(PgWriter writer, IPAddress value)
        => GaussDBInetConverter.WriteImpl(
            writer,
            (value, (byte)(value.AddressFamily == AddressFamily.InterNetwork ? 32 : 128)),
            isCidr: false);
}
