using System;
using System.ComponentModel;
using HuaweiCloud.GaussDB;

namespace Microsoft.Extensions.DependencyInjection;

public static partial class GaussDBServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="GaussDBDataSource" /> and an <see cref="GaussDBConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddGaussDBDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddGaussDBDataSourceCore(
            serviceCollection, serviceKey: null, connectionString, dataSourceBuilderAction: null,
            connectionLifetime, dataSourceLifetime, state: null);

    /// <summary>
    /// Registers an <see cref="GaussDBDataSource" /> and an <see cref="GaussDBConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="dataSourceBuilderAction">
    /// An action to configure the <see cref="GaussDBDataSourceBuilder" /> for further customizations of the <see cref="GaussDBDataSource" />.
    /// </param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddGaussDBDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        Action<GaussDBDataSourceBuilder> dataSourceBuilderAction,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddGaussDBDataSourceCore(serviceCollection, serviceKey: null, connectionString,
            static (_, builder, state) => ((Action<GaussDBDataSourceBuilder>)state!)(builder),
            connectionLifetime, dataSourceLifetime, state: dataSourceBuilderAction);

    /// <summary>
    /// Registers an <see cref="GaussDBDataSource" /> and an <see cref="GaussDBConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddGaussDBSlimDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddGaussDBSlimDataSourceCore(
            serviceCollection, serviceKey: null, connectionString, dataSourceBuilderAction: null,
            connectionLifetime, dataSourceLifetime, state: null);

    /// <summary>
    /// Registers an <see cref="GaussDBDataSource" /> and an <see cref="GaussDBConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="dataSourceBuilderAction">
    /// An action to configure the <see cref="GaussDBSlimDataSourceBuilder" /> for further customizations of the <see cref="GaussDBDataSource" />.
    /// </param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddGaussDBSlimDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        Action<GaussDBSlimDataSourceBuilder> dataSourceBuilderAction,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddGaussDBSlimDataSourceCore(serviceCollection, serviceKey: null, connectionString,
            static (_, builder, state) => ((Action<GaussDBSlimDataSourceBuilder>)state!)(builder),
            connectionLifetime, dataSourceLifetime, state: dataSourceBuilderAction);

    /// <summary>
    /// Registers an <see cref="GaussDBMultiHostDataSource" /> and an <see cref="GaussDBConnection" /> in the
    /// <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddMultiHostGaussDBDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddMultiHostGaussDBDataSourceCore(
            serviceCollection, serviceKey: null, connectionString, dataSourceBuilderAction: null,
            connectionLifetime, dataSourceLifetime, state: null);

    /// <summary>
    /// Registers an <see cref="GaussDBMultiHostDataSource" /> and an <see cref="GaussDBConnection" /> in the
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="dataSourceBuilderAction">
    /// An action to configure the <see cref="GaussDBDataSourceBuilder" /> for further customizations of the <see cref="GaussDBDataSource" />.
    /// </param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddMultiHostGaussDBDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        Action<GaussDBDataSourceBuilder> dataSourceBuilderAction,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddMultiHostGaussDBDataSourceCore(
            serviceCollection, serviceKey: null, connectionString,
            static (_, builder, state) => ((Action<GaussDBDataSourceBuilder>)state!)(builder),
            connectionLifetime, dataSourceLifetime, state: dataSourceBuilderAction);

    /// <summary>
    /// Registers an <see cref="GaussDBMultiHostDataSource" /> and an <see cref="GaussDBConnection" /> in the
    /// <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddMultiHostGaussDBSlimDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddMultiHostGaussDBSlimDataSourceCore(
            serviceCollection, serviceKey: null, connectionString, dataSourceBuilderAction: null,
            connectionLifetime, dataSourceLifetime, state: null);

    /// <summary>
    /// Registers an <see cref="GaussDBMultiHostDataSource" /> and an <see cref="GaussDBConnection" /> in the
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">An GaussDB connection string.</param>
    /// <param name="dataSourceBuilderAction">
    /// An action to configure the <see cref="GaussDBDataSourceBuilder" /> for further customizations of the <see cref="GaussDBDataSource" />.
    /// </param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="GaussDBConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="GaussDBDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never), Obsolete("Defined for binary compatibility with 7.0")]
    public static IServiceCollection AddMultiHostGaussDBSlimDataSource(
        this IServiceCollection serviceCollection,
        string connectionString,
        Action<GaussDBSlimDataSourceBuilder> dataSourceBuilderAction,
        ServiceLifetime connectionLifetime,
        ServiceLifetime dataSourceLifetime)
        => AddMultiHostGaussDBSlimDataSourceCore(
            serviceCollection, serviceKey: null, connectionString,
            static (_, builder, state) => ((Action<GaussDBSlimDataSourceBuilder>)state!)(builder),
            connectionLifetime, dataSourceLifetime, state: dataSourceBuilderAction);
}
