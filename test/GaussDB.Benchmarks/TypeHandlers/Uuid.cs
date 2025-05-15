using System;
using BenchmarkDotNet.Attributes;
using HuaweiCloud.GaussDB.Internal.Converters;

namespace HuaweiCloud.GaussDB.Benchmarks.TypeHandlers;

[Config(typeof(Config))]
public class Uuid() : TypeHandlerBenchmarks<Guid>(new GuidUuidConverter());
