using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql.BackendMessages;
using Npgsql.Util;
using static Npgsql.Util.Statics;

namespace Npgsql.Internal;

partial class NpgsqlConnector
{
    async Task Authenticate(string username, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken)
    {
        var requiredAuthModes = Settings.RequireAuthModes;
        if (requiredAuthModes == default)
            requiredAuthModes = NpgsqlConnectionStringBuilder.ParseAuthMode(PostgresEnvironment.RequireAuth);

        var authenticated = false;

        while (true)
        {
            timeout.CheckAndApply(this);
            var msg = ExpectAny<AuthenticationRequestMessage>(await ReadMessage(async).ConfigureAwait(false), this);
            switch (msg.AuthRequestType)
            {
            case AuthenticationRequestType.Ok:
                // If we didn't complete authentication, check whether it's allowed
                if (!authenticated)
                    ThrowIfNotAllowed(requiredAuthModes, RequireAuthMode.None);
                return;

            case AuthenticationRequestType.CleartextPassword:
                ThrowIfNotAllowed(requiredAuthModes, RequireAuthMode.Password);
                await AuthenticateCleartext(username, async, cancellationToken).ConfigureAwait(false);
                break;

            case AuthenticationRequestType.MD5Password:
                ThrowIfNotAllowed(requiredAuthModes, RequireAuthMode.MD5);
                await AuthenticateMD5(username, ((AuthenticationMD5PasswordMessage)msg).Salt, async, cancellationToken).ConfigureAwait(false);
                break;

            case AuthenticationRequestType.SHA256Password:
                ThrowIfNotAllowed(requiredAuthModes, RequireAuthMode.ScramSHA256);
                await AuthenticateSHA256(username, (AuthenticationSHA256PasswordMessage)msg, async, cancellationToken).ConfigureAwait(false);
                break;

            case AuthenticationRequestType.MD5SHA256Password:
                ThrowIfNotAllowed(requiredAuthModes, RequireAuthMode.MD5SHA256);
                await AuthenticateMD5SHA256(username, (AuthenticationMD5SHA256PasswordMessage)msg, async, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case AuthenticationRequestType.GSS:
            case AuthenticationRequestType.SSPI:
                ThrowIfNotAllowed(requiredAuthModes, msg.AuthRequestType == AuthenticationRequestType.GSS ? RequireAuthMode.GSS : RequireAuthMode.SSPI);
                await DataSource.IntegratedSecurityHandler.NegotiateAuthentication(async, this).ConfigureAwait(false);
                return;

            case AuthenticationRequestType.GSSContinue:
                throw new NpgsqlException("Can't start auth cycle with AuthenticationGSSContinue");

            default:
                throw new NotSupportedException($"Authentication method not supported (Received: {msg.AuthRequestType})");
            }

            authenticated = true;
        }

        static void ThrowIfNotAllowed(RequireAuthMode requiredAuthModes, RequireAuthMode requestedAuthMode)
        {
            if (!requiredAuthModes.HasFlag(requestedAuthMode))
                throw new NpgsqlException($"\"{requestedAuthMode}\" authentication method is not allowed. Allowed methods: {requiredAuthModes}");
        }
    }

    async Task AuthenticateCleartext(string username, bool async, CancellationToken cancellationToken = default)
    {
        var passwd = await GetPassword(username, async, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(passwd))
            throw new NpgsqlException("No password has been provided but the backend requires one (in cleartext)");

        var encoded = new byte[Encoding.UTF8.GetByteCount(passwd) + 1];
        Encoding.UTF8.GetBytes(passwd, 0, passwd.Length, encoded, 0);

        await WritePassword(encoded, async, cancellationToken).ConfigureAwait(false);
        await Flush(async, cancellationToken).ConfigureAwait(false);
    }

    internal void AuthenticateSASLSha256Plus(ref string mechanism, ref string cbindFlag, ref string cbind,
        ref bool successfulBind)
    {
        // The check below is copied from libpq (with commentary)
        // https://github.com/postgres/postgres/blob/98640f960eb9ed80cf90de3ef5d2e829b785b3eb/src/interfaces/libpq/fe-auth.c#L507-L517

        // The server offered SCRAM-SHA-256-PLUS, but the connection
        // is not SSL-encrypted. That's not sane. Perhaps SSL was
        // stripped by a proxy? There's no point in continuing,
        // because the server will reject the connection anyway if we
        // try authenticate without channel binding even though both
        // the client and server supported it. The SCRAM exchange
        // checks for that, to prevent downgrade attacks.
        if (!IsSecure)
            throw new NpgsqlException("Server offered SCRAM-SHA-256-PLUS authentication over a non-SSL connection");

        var sslStream = (SslStream)_stream;
        if (sslStream.RemoteCertificate is null)
        {
            ConnectionLogger.LogWarning("Remote certificate null, falling back to SCRAM-SHA-256");
            return;
        }

        // While SslStream.RemoteCertificate is X509Certificate2, it actually returns X509Certificate2
        // But to be on the safe side we'll just create a new instance of it
        using var remoteCertificate = new X509Certificate2(sslStream.RemoteCertificate);
        // Checking for hashing algorithms
        var algorithmName = remoteCertificate.SignatureAlgorithm.FriendlyName;

        HashAlgorithm? hashAlgorithm = algorithmName switch
        {
            not null when algorithmName.StartsWith("sha1", StringComparison.OrdinalIgnoreCase) => SHA256.Create(),
            not null when algorithmName.StartsWith("md5", StringComparison.OrdinalIgnoreCase) => SHA256.Create(),
            not null when algorithmName.StartsWith("sha256", StringComparison.OrdinalIgnoreCase) => SHA256.Create(),
            not null when algorithmName.StartsWith("sha384", StringComparison.OrdinalIgnoreCase) => SHA384.Create(),
            not null when algorithmName.StartsWith("sha512", StringComparison.OrdinalIgnoreCase) => SHA512.Create(),
            not null when algorithmName.StartsWith("sha3-256", StringComparison.OrdinalIgnoreCase) => SHA3_256.Create(),
            not null when algorithmName.StartsWith("sha3-384", StringComparison.OrdinalIgnoreCase) => SHA3_384.Create(),
            not null when algorithmName.StartsWith("sha3-512", StringComparison.OrdinalIgnoreCase) => SHA3_512.Create(),

            _ => null
        };

        if (hashAlgorithm is null)
        {
            ConnectionLogger.LogWarning(
                algorithmName is null
                    ? "Signature algorithm was null, falling back to SCRAM-SHA-256"
                    : $"Support for signature algorithm {algorithmName} is not yet implemented, falling back to SCRAM-SHA-256");
            return;
        }

        using var _ = hashAlgorithm;

        // RFC 5929
        mechanism = "SCRAM-SHA-256-PLUS";
        // PostgreSQL only supports tls-server-end-point binding
        cbindFlag = "p=tls-server-end-point";
        // SCRAM-SHA-256-PLUS depends on using ssl stream, so it's fine
        var cbindFlagBytes = Encoding.UTF8.GetBytes($"{cbindFlag},,");

        var certificateHash = hashAlgorithm.ComputeHash(remoteCertificate.GetRawCertData());
        var cbindBytes = new byte[cbindFlagBytes.Length + certificateHash.Length];
        cbindFlagBytes.CopyTo(cbindBytes, 0);
        certificateHash.CopyTo(cbindBytes, cbindFlagBytes.Length);
        cbind = Convert.ToBase64String(cbindBytes);
        successfulBind = true;
        IsScramPlus = true;
    }

    static byte[] Hi(string str, byte[] salt, int count)
        => Rfc2898DeriveBytes.Pbkdf2(str, salt, count, HashAlgorithmName.SHA256, 256 / 8);

    static byte[] Xor(byte[] buffer1, byte[] buffer2)
    {
        for (var i = 0; i < buffer1.Length; i++)
            buffer1[i] ^= buffer2[i];
        return buffer1;
    }

    static byte[] HMAC(byte[] key, string data) => HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(data));

    async Task AuthenticateMD5(string username, byte[] salt, bool async, CancellationToken cancellationToken = default)
    {
        var passwd = await GetPassword(username, async, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(passwd))
            throw new NpgsqlException("No password has been provided but the backend requires one (in MD5)");

        byte[] result;
        {
            // First phase
            var passwordBytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(passwd);
            var usernameBytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(username);
            var cryptBuf = new byte[passwordBytes.Length + usernameBytes.Length];
            passwordBytes.CopyTo(cryptBuf, 0);
            usernameBytes.CopyTo(cryptBuf, passwordBytes.Length);

            var sb = new StringBuilder();
            var hashResult = MD5.HashData(cryptBuf);
            foreach (var b in hashResult)
                sb.Append(b.ToString("x2"));

            var prehash = sb.ToString();

            var prehashbytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(prehash);
            cryptBuf = new byte[prehashbytes.Length + 4];

            Array.Copy(salt, 0, cryptBuf, prehashbytes.Length, 4);

            // 2.
            prehashbytes.CopyTo(cryptBuf, 0);

            sb = new StringBuilder("md5");
            hashResult = MD5.HashData(cryptBuf);
            foreach (var b in hashResult)
                sb.Append(b.ToString("x2"));

            var resultString = sb.ToString();
            result = new byte[Encoding.UTF8.GetByteCount(resultString) + 1];
            Encoding.UTF8.GetBytes(resultString, 0, resultString.Length, result, 0);
            result[^1] = 0;
        }

        await WritePassword(result, async, cancellationToken).ConfigureAwait(false);
        await Flush(async, cancellationToken).ConfigureAwait(false);
    }

    async Task AuthenticateSHA256(string username, AuthenticationSHA256PasswordMessage message, bool async, CancellationToken cancellationToken = default)
    {
        var password = await GetPassword(username, async, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(password))
            throw new NpgsqlException("No password has been provided but the backend requires one (in SHA256)");

        var normalizedPassword = password.Normalize(NormalizationForm.FormKC);
        var passwordBytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(normalizedPassword);

        var saltBytes = Convert.FromHexString(message.RandomCode);
        var tokenBytes = Convert.FromHexString(message.Token);

        var passwordKeyBytes = Rfc2898DeriveBytes.Pbkdf2(passwordBytes, saltBytes,
                message.Iteration, HashAlgorithmName.SHA1, 32
            );

        var clientKey = HMACSHA256.HashData(passwordKeyBytes, "Client Key"u8);
        var storedKey = SHA256.HashData(clientKey);
        var tokenKey = HMACSHA256.HashData(storedKey, tokenBytes);
        var hValue = Xor(tokenKey, clientKey);

        var result = new byte[hValue.Length  * 2 + 1];
        BytesToHex(hValue, result, 0, hValue.Length);
        await WritePassword(result, async, cancellationToken).ConfigureAwait(false);
        await Flush(async, cancellationToken).ConfigureAwait(false);
    }

    async Task AuthenticateMD5SHA256(string username, AuthenticationMD5SHA256PasswordMessage message, bool async, CancellationToken cancellationToken = default)
    {
        var password = await GetPassword(username, async, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(password))
            throw new NpgsqlException("No password has been provided but the backend requires one (in SHA256)");

        var passwordBytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(password);

        // https://github.com/HuaweiCloudDeveloper/gaussdb-r2dbc/blob/54783aa7ba09731300b31d9cf366185d0bf50447/src/main/java/io/r2dbc/gaussdb/util/MD5Digest.java#L227
        var randomCodeBytes = Convert.FromHexString(message.RandomCode);

        var passwordKeyBytes = Rfc2898DeriveBytes.Pbkdf2(passwordBytes, randomCodeBytes,
            2048, HashAlgorithmName.SHA1, 32
        );

        var serverKey = HMACSHA256.HashData(passwordKeyBytes, "Sever Key"u8);
        var clientKey = HMACSHA256.HashData(passwordKeyBytes, "Client Key"u8);
        var storedKey = SHA256.HashData(clientKey);

        var stringBuilder = new StringBuilder();
        stringBuilder.Append(message.RandomCode);
        stringBuilder.Append(Convert.ToHexString(serverKey).ToLowerInvariant());
        stringBuilder.Append(Convert.ToHexString(storedKey).ToLowerInvariant());
        var encryptedString = stringBuilder.ToString();

        byte[] passDigest;
        using (var md5 = MD5.Create())
        {
            // Convert the string to bytes using UTF-8 encoding
            var stringBytes = NpgsqlWriteBuffer.UTF8Encoding.GetBytes(encryptedString);

            // Update the hash state with the string bytes
            // The 'null, 0' arguments are for the output buffer, which isn't needed here
            md5.TransformBlock(stringBytes, 0, stringBytes.Length, null, 0);

            // Update the hash state with the salt bytes and finalize the hash calculation
            // This is the final block of data being added.
            var saltBytes = message.Salt.ToArray();
            md5.TransformFinalBlock(saltBytes, 0, saltBytes.Length);

            // Retrieve the computed hash digest
            ArgumentNullException.ThrowIfNull(md5.Hash);
            passDigest = md5.Hash;
        }

        var result = new byte[MD5.HashSizeInBytes * 2 + 3];
        result[0] = (byte)'m';
        result[1] = (byte)'d';
        result[2] = (byte)'5';
        BytesToHex(passDigest, result, 3, MD5.HashSizeInBytes);
        await WritePassword(result, async, cancellationToken).ConfigureAwait(false);
        await Flush(async, cancellationToken).ConfigureAwait(false);
    }

    internal async Task AuthenticateGSS(bool async)
    {
        var targetName = $"{KerberosServiceName}/{Host}";

        var clientOptions = new NegotiateAuthenticationClientOptions { TargetName = targetName };
        NegotiateOptionsCallback?.Invoke(clientOptions);

        using var authContext = new NegotiateAuthentication(clientOptions);
        var data = authContext.GetOutgoingBlob(ReadOnlySpan<byte>.Empty, out var statusCode)!;
        Debug.Assert(statusCode == NegotiateAuthenticationStatusCode.ContinueNeeded);
        await WritePassword(data, 0, data.Length, async, UserCancellationToken).ConfigureAwait(false);
        await Flush(async, UserCancellationToken).ConfigureAwait(false);
        while (true)
        {
            var response = ExpectAny<AuthenticationRequestMessage>(await ReadMessage(async).ConfigureAwait(false), this);
            if (response.AuthRequestType == AuthenticationRequestType.Ok)
                break;
            if (response is not AuthenticationGSSContinueMessage gssMsg)
                throw new NpgsqlException($"Received unexpected authentication request message {response.AuthRequestType}");
            data = authContext.GetOutgoingBlob(gssMsg.AuthenticationData.AsSpan(), out statusCode)!;
            if (statusCode is not NegotiateAuthenticationStatusCode.Completed and not NegotiateAuthenticationStatusCode.ContinueNeeded)
                throw new NpgsqlException($"Error while authenticating GSS/SSPI: {statusCode}");
            // We might get NegotiateAuthenticationStatusCode.Completed but the data will not be null
            // This can happen if it's the first cycle, in which case we have to send that data to complete handshake (#4888)
            if (data is null)
                continue;
            await WritePassword(data, 0, data.Length, async, UserCancellationToken).ConfigureAwait(false);
            await Flush(async, UserCancellationToken).ConfigureAwait(false);
        }
    }

    async ValueTask<string?> GetPassword(string username, bool async, CancellationToken cancellationToken = default)
    {
        var password = await DataSource.GetPassword(async, cancellationToken).ConfigureAwait(false);

        if (password is not null)
            return password;

        if (ProvidePasswordCallback is { } passwordCallback)
        {
            try
            {
                ConnectionLogger.LogTrace($"Taking password from {nameof(ProvidePasswordCallback)} delegate");
                password = passwordCallback(Host, Port, Settings.Database!, username);
            }
            catch (Exception e)
            {
                throw new NpgsqlException($"Obtaining password using {nameof(NpgsqlConnection)}.{nameof(ProvidePasswordCallback)} delegate failed", e);
            }
        }

        password ??= PostgresEnvironment.Password;

        if (password != null)
            return password;

        var passFile = Settings.Passfile ?? PostgresEnvironment.PassFile ?? PostgresEnvironment.PassFileDefault;
        if (passFile != null)
        {
            var matchingEntry = new PgPassFile(passFile!)
                .GetFirstMatchingEntry(Host, Port, Settings.Database!, username);
            if (matchingEntry != null)
            {
                ConnectionLogger.LogTrace("Taking password from pgpass file");
                password = matchingEntry.Password;
            }
        }

        return password;
    }

    static void BytesToHex(byte[] bytes, byte[] hex, int offset, int length)
    {
        var lookup = new[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
        var pos = offset;
        for (var i = 0; i < length; ++i)
        {
            var c = bytes[i] & 255;
            var j = c >> 4;
            hex[pos++] = (byte)lookup[j];
            j = c & 15;
            hex[pos++] = (byte)lookup[j];
        }
    }
}
