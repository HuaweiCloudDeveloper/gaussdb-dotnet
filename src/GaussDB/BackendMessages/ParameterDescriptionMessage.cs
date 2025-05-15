using System.Collections.Generic;
using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB.BackendMessages;

sealed class ParameterDescriptionMessage : IBackendMessage
{
    // ReSharper disable once InconsistentNaming
    internal List<uint> TypeOIDs { get; }

    internal ParameterDescriptionMessage()
        => TypeOIDs = [];

    internal ParameterDescriptionMessage Load(GaussDBReadBuffer buf)
    {
        var numParams = buf.ReadUInt16();
        TypeOIDs.Clear();
        for (var i = 0; i < numParams; i++)
            TypeOIDs.Add(buf.ReadUInt32());
        return this;
    }

    public BackendMessageCode Code => BackendMessageCode.ParameterDescription;
}
