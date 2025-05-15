using System;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using GaussDB.Internal;

namespace GaussDB;

sealed record GaussDBDataSourceConfiguration(string? Name,
    GaussDBLoggingConfiguration LoggingConfiguration,
    GaussDBTracingOptions TracingOptions,
    GaussDBTypeLoadingOptions TypeLoading,
    TransportSecurityHandler TransportSecurityHandler,
    IntegratedSecurityHandler IntegratedSecurityHandler,
    Action<SslClientAuthenticationOptions>? SslClientAuthenticationOptionsCallback,
    Func<GaussDBConnectionStringBuilder, string>? PasswordProvider,
    Func<GaussDBConnectionStringBuilder, CancellationToken, ValueTask<string>>? PasswordProviderAsync,
    Func<GaussDBConnectionStringBuilder, CancellationToken, ValueTask<string>>? PeriodicPasswordProvider,
    TimeSpan PeriodicPasswordSuccessRefreshInterval,
    TimeSpan PeriodicPasswordFailureRefreshInterval,
    PgTypeInfoResolverChain ResolverChain,
    IGaussDBNameTranslator DefaultNameTranslator,
    Action<GaussDBConnection>? ConnectionInitializer,
    Func<GaussDBConnection, Task>? ConnectionInitializerAsync,
    Action<NegotiateAuthenticationClientOptions>? NegotiateOptionsCallback);
