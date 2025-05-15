using System;
using BenchmarkDotNet.Attributes;
using GaussDB.Internal.Converters;

namespace GaussDB.Benchmarks.TypeHandlers;

[Config(typeof(Config))]
public class Uuid() : TypeHandlerBenchmarks<Guid>(new GuidUuidConverter());
