using System;
using Npgsql.Internal;

namespace Npgsql.BackendMessages;

abstract class AuthenticationRequestMessage : IBackendMessage
{
    public BackendMessageCode Code => BackendMessageCode.AuthenticationRequest;
    internal abstract AuthenticationRequestType AuthRequestType { get; }
}

sealed class AuthenticationOkMessage : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.Ok;

    internal static readonly AuthenticationOkMessage Instance = new();
    AuthenticationOkMessage() { }
}

sealed class AuthenticationCleartextPasswordMessage  : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.CleartextPassword;

    internal static readonly AuthenticationCleartextPasswordMessage Instance = new();
    AuthenticationCleartextPasswordMessage() { }
}

sealed class AuthenticationMD5PasswordMessage  : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.MD5Password;

    internal byte[] Salt { get; }

    internal static AuthenticationMD5PasswordMessage Load(NpgsqlReadBuffer buf)
    {
        var salt = new byte[4];
        buf.ReadBytes(salt, 0, 4);
        return new AuthenticationMD5PasswordMessage(salt);
    }

    AuthenticationMD5PasswordMessage(byte[] salt)
        => Salt = salt;
}

#region SHA256Password

sealed class AuthenticationSHA256PasswordMessage : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.SHA256Password;
    internal PasswordStoreType PasswordStoreType { get; }
    internal string RandomCode { get; }
    internal string Token { get; set; }
    internal int Iteration { get; }

    public AuthenticationSHA256PasswordMessage
    (
        PasswordStoreType passwordStoreType,
        NpgsqlReadBuffer buf
    )
    {
        PasswordStoreType = passwordStoreType;
        RandomCode = buf.ReadString(64);
        Token = buf.ReadString(8);
        Iteration = buf.ReadInt32();
    }

    internal static AuthenticationRequestMessage Load(NpgsqlReadBuffer buf)
    {
        var passwordStoreTypeValue = buf.ReadInt32();
        var passwordStoreType = (PasswordStoreType)passwordStoreTypeValue;

        return passwordStoreType switch
        {
            PasswordStoreType.PlainText => AuthenticationCleartextPasswordMessage.Instance,
            PasswordStoreType.MD5 => AuthenticationMD5PasswordMessage.Load(buf),
            PasswordStoreType.SHA256 => new AuthenticationSHA256PasswordMessage(
                passwordStoreType, buf
            ),
            _ => throw new InvalidOperationException($"Not supported password store type({passwordStoreTypeValue})")
        };
    }
}

#endregion SHA256Password


sealed class AuthenticationGSSMessage : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.GSS;

    internal static readonly AuthenticationGSSMessage Instance = new();
    AuthenticationGSSMessage() { }
}

sealed class AuthenticationGSSContinueMessage : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.GSSContinue;

    internal byte[] AuthenticationData { get; }

    internal static AuthenticationGSSContinueMessage Load(NpgsqlReadBuffer buf, int len)
    {
        len -= 4;   // The AuthRequestType code
        var authenticationData = new byte[len];
        buf.ReadBytes(authenticationData, 0, len);
        return new AuthenticationGSSContinueMessage(authenticationData);
    }

    AuthenticationGSSContinueMessage(byte[] authenticationData)
        => AuthenticationData = authenticationData;
}

sealed class AuthenticationSSPIMessage : AuthenticationRequestMessage
{
    internal override AuthenticationRequestType AuthRequestType => AuthenticationRequestType.SSPI;

    internal static readonly AuthenticationSSPIMessage Instance = new();
    AuthenticationSSPIMessage() { }
}

enum AuthenticationRequestType
{
    Ok = 0,
    CleartextPassword = 3,
    MD5Password = 5,
    GSS = 7,
    GSSContinue = 8,
    SSPI = 9,
    SHA256Password = 10
}

enum PasswordStoreType
{
    PlainText = 0,
    MD5 = 1,
    SHA256 = 2
}
