

/* Disabling for now: unmapped composite support is probably going away, and there's a good chance this
 * class can be simplified to a certain extent
namespace HuaweiCloud.GaussDB.Benchmarks.TypeHandlers
{
    public abstract class Composite<T> : TypeHandlerBenchmarks<T>
    {
        internal static readonly ConnectorTypeMapper TypeMapper;

        internal static readonly GaussDBDatabaseInfo DatabaseInfo;

        internal static GaussDBSnakeCaseNameTranslator NameTranslator;

        static Composite()
        {
            DatabaseInfo = new TestDatabaseInfo();
            DatabaseInfo.ProcessTypes();

            var connection = new GaussDBConnection();
            var connector = connection.Connector = new GaussDBConnector(connection);
            TypeMapper = connector.TypeMapper = new ConnectorTypeMapper(connector);

            connector.State = ConnectorState.Ready;

            var textMapping = TypeMapper.Mappings["text"];
            TypeMapper.Bind(DatabaseInfo);
            TypeMapper.AddMapping(
                new GaussDBTypeMappingBuilder
                {
                    ClrTypes = textMapping.ClrTypes,
                    DbTypes = textMapping.DbTypes,
                    InferredDbType = textMapping.InferredDbType,
                    GaussDBDbType = textMapping.GaussDBDbType,
                    PgTypeName = textMapping.PgTypeName,
                    TypeHandlerFactory = new TestTextHandlerFactory()
                }.Build());

            NameTranslator = new GaussDBSnakeCaseNameTranslator();
        }

        protected Composite(GaussDBTypeHandler handler) : base(handler) { }

        class TestDatabaseInfo : PostgresDatabaseInfo
        {
            internal TestDatabaseInfo(GaussDBConnection conn) : base(conn) {}

            static readonly PostgresBaseType[] Types =
            {
                new PostgresBaseType("pg_catalog", "integer", 23),
                new PostgresBaseType("pg_catalog", "text", 25),
                new PostgresBaseType("pg_catalog", "double precision", 701),
            };

            protected override IEnumerable<PostgresType> GetTypes() => Types;
        }

        class TestTextHandlerFactory : GaussDBTypeHandlerFactory<string>
        {
            public override GaussDBTypeHandler<string> Create(PostgresType postgresType, GaussDBConnection conn)
                => new TextHandler(postgresType, PGUtil.UTF8Encoding);
        }
    }

    public abstract class ClassComposite : Composite<ClassComposite.TestClass>
    {
        internal static readonly PostgresCompositeType CompositeType;

        static ClassComposite()
        {
            CompositeType = new PostgresCompositeType("public", "test_class", 1);
            CompositeType.MutableFields.Add(new PostgresCompositeType.Field("age", DatabaseInfo.ByName["integer"]));
            CompositeType.MutableFields.Add(new PostgresCompositeType.Field("first_name", DatabaseInfo.ByName["text"]));
            CompositeType.MutableFields.Add(new PostgresCompositeType.Field("last_name", DatabaseInfo.ByName["text"]));
        }

        protected ClassComposite(GaussDBTypeHandler handler)
            : base(handler) { }

        protected override IEnumerable<TestClass> ValuesOverride()
        {
            yield return new TestClass
            {
                Age = 30,
                FirstName = "John",
                LastName = "Smith"
            };
        }

        public class TestClass
        {
            public int Age { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }
    }

    public abstract class StructComposite : Composite<StructComposite.TestStruct>
    {
        internal static readonly PostgresCompositeType CompositeType;

        static StructComposite()
        {
            CompositeType = new PostgresCompositeType("public", "test_struct", 1);
            CompositeType.MutableFields.Add(new PostgresCompositeType.Field("latitude", DatabaseInfo.ByName["double precision"]));
            CompositeType.MutableFields.Add(new PostgresCompositeType.Field("longitude", DatabaseInfo.ByName["double precision"]));
        }

        protected StructComposite(GaussDBTypeHandler handler)
            : base(handler) { }

        protected override IEnumerable<TestStruct> ValuesOverride()
        {
            yield return new TestStruct
            {
                Latitude = 55.083333,
                Longitude = 38.783333
            };
        }

        public class TestStruct
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }
    }

    [Config(typeof(Config))]
    public class MappedClassComposite : ClassComposite
    {
        public MappedClassComposite() : base(MappedCompositeHandler<TestClass>.Create(CompositeType, TypeMapper, NameTranslator)) { }
    }

    [Config(typeof(Config))]
    public class MappedStructComposite : StructComposite
    {
        public MappedStructComposite() : base(MappedCompositeHandler<TestStruct>.Create(CompositeType, TypeMapper, NameTranslator)) { }
    }

    [Config(typeof(Config))]
    public class UnmappedClassComposite : ClassComposite
    {
        public UnmappedClassComposite() : base(new UnmappedCompositeHandler(NameTranslator, TypeMapper) { PostgresType = CompositeType }) { }
    }

    [Config(typeof(Config))]
    public class UnmappedStructComposite : StructComposite
    {
        public UnmappedStructComposite() : base(new UnmappedCompositeHandler(NameTranslator, TypeMapper) { PostgresType = CompositeType }) { }
    }
}
*/
