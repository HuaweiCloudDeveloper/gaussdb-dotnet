using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using HuaweiCloud.GaussDB.Internal.Postgres;

namespace HuaweiCloud.GaussDB.Tests;

public class GaussDBParameterTest : TestBase
{
    [Test, Description("Makes sure that when GaussDBDbType or Value/GaussDBValue are set, DbType and GaussDBDbType are set accordingly")]
    public void Implicit_setting_of_DbType()
    {
        var p = new GaussDBParameter("p", DbType.Int32);
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Integer));

        // As long as GaussDBDbType/DbType aren't set explicitly, infer them from Value
        p = new GaussDBParameter("p", 8);
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Integer));
        Assert.That(p.DbType, Is.EqualTo(DbType.Int32));

        p.Value = 3.0;
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Double));
        Assert.That(p.DbType, Is.EqualTo(DbType.Double));

        p.GaussDBDbType = GaussDBDbType.Bytea;
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Bytea));
        Assert.That(p.DbType, Is.EqualTo(DbType.Binary));

        p.Value = "dont_change";
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Bytea));
        Assert.That(p.DbType, Is.EqualTo(DbType.Binary));

        p = new GaussDBParameter("p", new int[0]);
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Array | GaussDBDbType.Integer));
        Assert.That(p.DbType, Is.EqualTo(DbType.Object));
    }

    [Test]
    public void DataTypeName()
    {
        using var conn = OpenConnection();
        using var cmd = new GaussDBCommand("SELECT @p", conn);
        var p1 = new GaussDBParameter { ParameterName = "p", Value = 8, DataTypeName = "integer" };
        cmd.Parameters.Add(p1);
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(8));
        // Purposefully try to send int as string, which should fail. This makes sure
        // the above doesn't work simply because of type inference from the CLR type.
        p1.DataTypeName = "text";
        Assert.That(() => cmd.ExecuteScalar(), Throws.Exception.TypeOf<InvalidCastException>());

        cmd.Parameters.Clear();

        var p2 = new GaussDBParameter<int> { ParameterName = "p", TypedValue = 8, DataTypeName = "integer" };
        cmd.Parameters.Add(p2);
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(8));
        // Purposefully try to send int as string, which should fail. This makes sure
        // the above doesn't work simply because of type inference from the CLR type.
        p2.DataTypeName = "text";
        Assert.That(() => cmd.ExecuteScalar(), Throws.Exception.TypeOf<InvalidCastException>());
    }

    [Test]
    public void Positional_parameter_is_positional()
    {
        var p = new GaussDBParameter(GaussDBParameter.PositionalName, 1);
        Assert.That(p.IsPositional, Is.True);

        var p2 = new GaussDBParameter(null, 1);
        Assert.That(p2.IsPositional, Is.True);
    }

    [Test]
    public void Infer_data_type_name_from_GaussDBDbType()
    {
        var p = new GaussDBParameter("par_field1", GaussDBDbType.Varchar, 50);
        Assert.That(p.DataTypeName, Is.EqualTo("character varying"));
    }

    [Test]
    public void Infer_data_type_name_from_DbType()
    {
        var p = new GaussDBParameter("par_field1", DbType.String , 50);
        Assert.That(p.DataTypeName, Is.EqualTo("text"));
    }

    [Test]
    public void Infer_data_type_name_from_GaussDBDbType_for_array()
    {
        var p = new GaussDBParameter("int_array", GaussDBDbType.Array | GaussDBDbType.Integer);
        Assert.That(p.DataTypeName, Is.EqualTo("integer[]"));
    }

    [Test]
    public void Infer_data_type_name_from_GaussDBDbType_for_built_in_range()
    {
        var p = new GaussDBParameter("numeric_range", GaussDBDbType.Range | GaussDBDbType.Numeric);
        Assert.That(p.DataTypeName, Is.EqualTo("numrange"));
    }

    [Test]
    public void Cannot_infer_data_type_name_from_GaussDBDbType_for_unknown_range()
    {
        var p = new GaussDBParameter("text_range", GaussDBDbType.Range | GaussDBDbType.Text);
        Assert.That(p.DataTypeName, Is.EqualTo(null));
    }

    [Test]
    public void Infer_data_type_name_from_ClrType()
    {
        var p = new GaussDBParameter("p1", Array.Empty<byte>());
        Assert.That(p.DataTypeName, Is.EqualTo("bytea"));
    }

    [Test]
    public void Setting_DbType_sets_GaussDBDbType()
    {
        var p = new GaussDBParameter();
        p.DbType = DbType.Binary;
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Bytea));
    }

    [Test]
    public void Setting_GaussDBDbType_sets_DbType()
    {
        var p = new GaussDBParameter();
        p.GaussDBDbType = GaussDBDbType.Bytea;
        Assert.That(p.DbType, Is.EqualTo(DbType.Binary));
    }

    [Test]
    public void Setting_value_does_not_change_DbType()
    {
        var p = new GaussDBParameter { DbType = DbType.String, GaussDBDbType = GaussDBDbType.Bytea };
        p.Value = 8;
        Assert.That(p.DbType, Is.EqualTo(DbType.Binary));
        Assert.That(p.GaussDBDbType, Is.EqualTo(GaussDBDbType.Bytea));
    }

    // Older tests

    #region Constructors

    [Test]
    public void Constructor1()
    {
        var p = new GaussDBParameter();
        Assert.AreEqual(DbType.Object, p.DbType, "DbType");
        Assert.AreEqual(ParameterDirection.Input, p.Direction, "Direction");
        Assert.IsFalse(p.IsNullable, "IsNullable");
        Assert.AreEqual(string.Empty, p.ParameterName, "ParameterName");
        Assert.AreEqual(0, p.Precision, "Precision");
        Assert.AreEqual(0, p.Scale, "Scale");
        Assert.AreEqual(0, p.Size, "Size");
        Assert.AreEqual(string.Empty, p.SourceColumn, "SourceColumn");
        Assert.AreEqual(DataRowVersion.Current, p.SourceVersion, "SourceVersion");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "GaussDBDbType");
        Assert.IsNull(p.Value, "Value");
    }

    [Test]
    public void Constructor2_Value_DateTime()
    {
        var value = new DateTime(2004, 8, 24);

        var p = new GaussDBParameter("address", value);
        Assert.AreEqual(DbType.DateTime2, p.DbType, "B:DbType");
        Assert.AreEqual(ParameterDirection.Input, p.Direction, "B:Direction");
        Assert.IsFalse(p.IsNullable, "B:IsNullable");
        Assert.AreEqual("address", p.ParameterName, "B:ParameterName");
        Assert.AreEqual(0, p.Precision, "B:Precision");
        Assert.AreEqual(0, p.Scale, "B:Scale");
        //Assert.AreEqual (0, p.Size, "B:Size");
        Assert.AreEqual(string.Empty, p.SourceColumn, "B:SourceColumn");
        Assert.AreEqual(DataRowVersion.Current, p.SourceVersion, "B:SourceVersion");
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "B:GaussDBDbType");
        Assert.AreEqual(value, p.Value, "B:Value");
    }

    [Test]
    public void Constructor2_Value_DBNull()
    {
        var p = new GaussDBParameter("address", DBNull.Value);
        Assert.AreEqual(DbType.Object, p.DbType, "B:DbType");
        Assert.AreEqual(ParameterDirection.Input, p.Direction, "B:Direction");
        Assert.IsFalse(p.IsNullable, "B:IsNullable");
        Assert.AreEqual("address", p.ParameterName, "B:ParameterName");
        Assert.AreEqual(0, p.Precision, "B:Precision");
        Assert.AreEqual(0, p.Scale, "B:Scale");
        Assert.AreEqual(0, p.Size, "B:Size");
        Assert.AreEqual(string.Empty, p.SourceColumn, "B:SourceColumn");
        Assert.AreEqual(DataRowVersion.Current, p.SourceVersion, "B:SourceVersion");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "B:GaussDBDbType");
        Assert.AreEqual(DBNull.Value, p.Value, "B:Value");
    }

    [Test]
    public void Constructor2_Value_null()
    {
        var p = new GaussDBParameter("address", null);
        Assert.AreEqual(DbType.Object, p.DbType, "A:DbType");
        Assert.AreEqual(ParameterDirection.Input, p.Direction, "A:Direction");
        Assert.IsFalse(p.IsNullable, "A:IsNullable");
        Assert.AreEqual("address", p.ParameterName, "A:ParameterName");
        Assert.AreEqual(0, p.Precision, "A:Precision");
        Assert.AreEqual(0, p.Scale, "A:Scale");
        Assert.AreEqual(0, p.Size, "A:Size");
        Assert.AreEqual(string.Empty, p.SourceColumn, "A:SourceColumn");
        Assert.AreEqual(DataRowVersion.Current, p.SourceVersion, "A:SourceVersion");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "A:GaussDBDbType");
        Assert.IsNull(p.Value, "A:Value");
    }

    [Test]
    //.ctor (String, GaussDBDbType, Int32, String, ParameterDirection, bool, byte, byte, DataRowVersion, object)
    public void Constructor7()
    {
        var p1 = new GaussDBParameter("p1Name", GaussDBDbType.Varchar, 20,
            "srcCol", ParameterDirection.InputOutput, false, 0, 0,
            DataRowVersion.Original, "foo");
        Assert.AreEqual(DbType.String, p1.DbType, "DbType");
        Assert.AreEqual(ParameterDirection.InputOutput, p1.Direction, "Direction");
        Assert.AreEqual(false, p1.IsNullable, "IsNullable");
        //Assert.AreEqual (999, p1.LocaleId, "#");
        Assert.AreEqual("p1Name", p1.ParameterName, "ParameterName");
        Assert.AreEqual(0, p1.Precision, "Precision");
        Assert.AreEqual(0, p1.Scale, "Scale");
        Assert.AreEqual(20, p1.Size, "Size");
        Assert.AreEqual("srcCol", p1.SourceColumn, "SourceColumn");
        Assert.AreEqual(false, p1.SourceColumnNullMapping, "SourceColumnNullMapping");
        Assert.AreEqual(DataRowVersion.Original, p1.SourceVersion, "SourceVersion");
        Assert.AreEqual(GaussDBDbType.Varchar, p1.GaussDBDbType, "GaussDBDbType");
        //Assert.AreEqual (3210, p1.GaussDBValue, "#");
        Assert.AreEqual("foo", p1.Value, "Value");
        //Assert.AreEqual ("database", p1.XmlSchemaCollectionDatabase, "XmlSchemaCollectionDatabase");
        //Assert.AreEqual ("name", p1.XmlSchemaCollectionName, "XmlSchemaCollectionName");
        //Assert.AreEqual ("schema", p1.XmlSchemaCollectionOwningSchema, "XmlSchemaCollectionOwningSchema");
    }

    [Test]
    public void Clone()
    {
        var expected = new GaussDBParameter
        {
            Value = 42,
            ParameterName = "TheAnswer",

            DbType = DbType.Int32,
            GaussDBDbType = GaussDBDbType.Integer,
            DataTypeName = "integer",

            Direction = ParameterDirection.InputOutput,
            IsNullable = true,
            Precision = 1,
            Scale = 2,
            Size = 4,

            SourceVersion = DataRowVersion.Proposed,
            SourceColumn = "source",
            SourceColumnNullMapping = true,
        };
        var actual = expected.Clone();

        Assert.AreEqual(expected.Value, actual.Value);
        Assert.AreEqual(expected.ParameterName, actual.ParameterName);

        Assert.AreEqual(expected.DbType, actual.DbType);
        Assert.AreEqual(expected.GaussDBDbType, actual.GaussDBDbType);
        Assert.AreEqual(expected.DataTypeName, actual.DataTypeName);

        Assert.AreEqual(expected.Direction, actual.Direction);
        Assert.AreEqual(expected.IsNullable, actual.IsNullable);
        Assert.AreEqual(expected.Precision, actual.Precision);
        Assert.AreEqual(expected.Scale, actual.Scale);
        Assert.AreEqual(expected.Size, actual.Size);

        Assert.AreEqual(expected.SourceVersion, actual.SourceVersion);
        Assert.AreEqual(expected.SourceColumn, actual.SourceColumn);
        Assert.AreEqual(expected.SourceColumnNullMapping, actual.SourceColumnNullMapping);
    }

    [Test]
    public void Clone_generic()
    {
        var expected = new GaussDBParameter<int>
        {
            TypedValue = 42,
            ParameterName = "TheAnswer",

            DbType = DbType.Int32,
            GaussDBDbType = GaussDBDbType.Integer,
            DataTypeName = "integer",

            Direction = ParameterDirection.InputOutput,
            IsNullable = true,
            Precision = 1,
            Scale = 2,
            Size = 4,

            SourceVersion = DataRowVersion.Proposed,
            SourceColumn ="source",
            SourceColumnNullMapping = true,
        };
        var actual = (GaussDBParameter<int>)expected.Clone();

        Assert.AreEqual(expected.Value, actual.Value);
        Assert.AreEqual(expected.TypedValue, actual.TypedValue);
        Assert.AreEqual(expected.ParameterName, actual.ParameterName);

        Assert.AreEqual(expected.DbType, actual.DbType);
        Assert.AreEqual(expected.GaussDBDbType, actual.GaussDBDbType);
        Assert.AreEqual(expected.DataTypeName, actual.DataTypeName);

        Assert.AreEqual(expected.Direction, actual.Direction);
        Assert.AreEqual(expected.IsNullable, actual.IsNullable);
        Assert.AreEqual(expected.Precision, actual.Precision);
        Assert.AreEqual(expected.Scale, actual.Scale);
        Assert.AreEqual(expected.Size, actual.Size);

        Assert.AreEqual(expected.SourceVersion, actual.SourceVersion);
        Assert.AreEqual(expected.SourceColumn, actual.SourceColumn);
        Assert.AreEqual(expected.SourceColumnNullMapping, actual.SourceColumnNullMapping);
    }

    #endregion

    [Test]
    [Ignore("")]
    public void InferType_invalid_throws()
    {
        var notsupported = new object[]
        {
            ushort.MaxValue,
            uint.MaxValue,
            ulong.MaxValue,
            sbyte.MaxValue,
            new GaussDBParameter()
        };

        var param = new GaussDBParameter();

        for (var i = 0; i < notsupported.Length; i++)
        {
            try
            {
                param.Value = notsupported[i];
                Assert.Fail("#A1:" + i);
            }
            catch (FormatException)
            {
                // appears to be bug in .NET 1.1 while
                // constructing exception message
            }
            catch (ArgumentException ex)
            {
                // The parameter data type of ... is invalid
                Assert.AreEqual(typeof(ArgumentException), ex.GetType(), "#A2");
                Assert.IsNull(ex.InnerException, "#A3");
                Assert.IsNotNull(ex.Message, "#A4");
                Assert.IsNull(ex.ParamName, "#A5");
            }
        }
    }

    [Test] // bug #320196
    public void Parameter_null()
    {
        var param = new GaussDBParameter("param", GaussDBDbType.Numeric);
        Assert.AreEqual(0, param.Scale, "#A1");
        param.Value = DBNull.Value;
        Assert.AreEqual(0, param.Scale, "#A2");

        param = new GaussDBParameter("param", GaussDBDbType.Integer);
        Assert.AreEqual(0, param.Scale, "#B1");
        param.Value = DBNull.Value;
        Assert.AreEqual(0, param.Scale, "#B2");
    }

    [Test]
    [Ignore("")]
    public void Parameter_type()
    {
        GaussDBParameter p;

        // If Type is not set, then type is inferred from the value
        // assigned. The Type should be inferred everytime Value is assigned
        // If value is null or DBNull, then the current Type should be reset to Text.
        p = new GaussDBParameter();
        Assert.AreEqual(DbType.String, p.DbType, "#A1");
        Assert.AreEqual(GaussDBDbType.Text, p.GaussDBDbType, "#A2");
        p.Value = DBNull.Value;
        Assert.AreEqual(DbType.String, p.DbType, "#B1");
        Assert.AreEqual(GaussDBDbType.Text, p.GaussDBDbType, "#B2");
        p.Value = 1;
        Assert.AreEqual(DbType.Int32, p.DbType, "#C1");
        Assert.AreEqual(GaussDBDbType.Integer, p.GaussDBDbType, "#C2");
        p.Value = DBNull.Value;
        Assert.AreEqual(DbType.String, p.DbType, "#D1");
        Assert.AreEqual(GaussDBDbType.Text, p.GaussDBDbType, "#D2");
        p.Value = new byte[] { 0x0a };
        Assert.AreEqual(DbType.Binary, p.DbType, "#E1");
        Assert.AreEqual(GaussDBDbType.Bytea, p.GaussDBDbType, "#E2");
        p.Value = null;
        Assert.AreEqual(DbType.String, p.DbType, "#F1");
        Assert.AreEqual(GaussDBDbType.Text, p.GaussDBDbType, "#F2");
        p.Value = DateTime.Now;
        Assert.AreEqual(DbType.DateTime, p.DbType, "#G1");
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#G2");
        p.Value = null;
        Assert.AreEqual(DbType.String, p.DbType, "#H1");
        Assert.AreEqual(GaussDBDbType.Text, p.GaussDBDbType, "#H2");

        // If DbType is set, then the GaussDBDbType should not be
        // inferred from the value assigned.
        p = new GaussDBParameter();
        p.DbType = DbType.DateTime;
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#I1");
        p.Value = 1;
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#I2");
        p.Value = null;
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#I3");
        p.Value = DBNull.Value;
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#I4");

        // If GaussDBDbType is set, then the DbType should not be
        // inferred from the value assigned.
        p = new GaussDBParameter();
        p.GaussDBDbType = GaussDBDbType.Bytea;
        Assert.AreEqual(GaussDBDbType.Bytea, p.GaussDBDbType, "#J1");
        p.Value = 1;
        Assert.AreEqual(GaussDBDbType.Bytea, p.GaussDBDbType, "#J2");
        p.Value = null;
        Assert.AreEqual(GaussDBDbType.Bytea, p.GaussDBDbType, "#J3");
        p.Value = DBNull.Value;
        Assert.AreEqual(GaussDBDbType.Bytea, p.GaussDBDbType, "#J4");
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/5428")]
    public async Task Match_param_index_case_insensitively()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new GaussDBCommand("SELECT @p,@P", conn);
        cmd.Parameters.AddWithValue("p", "Hello world");
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    [Ignore("")]
    public void ParameterName()
    {
        var p = new GaussDBParameter();
        p.ParameterName = "name";
        Assert.AreEqual("name", p.ParameterName, "#A:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#A:SourceColumn");

        p.ParameterName = null;
        Assert.AreEqual(string.Empty, p.ParameterName, "#B:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#B:SourceColumn");

        p.ParameterName = " ";
        Assert.AreEqual(" ", p.ParameterName, "#C:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#C:SourceColumn");

        p.ParameterName = " name ";
        Assert.AreEqual(" name ", p.ParameterName, "#D:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#D:SourceColumn");

        p.ParameterName = string.Empty;
        Assert.AreEqual(string.Empty, p.ParameterName, "#E:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#E:SourceColumn");
    }

    [Test]
    public void ResetDbType()
    {
        GaussDBParameter p;

        //Parameter with an assigned value but no DbType specified
        p = new GaussDBParameter("foo", 42);
        p.ResetDbType();
        Assert.AreEqual(DbType.Int32, p.DbType, "#A:DbType");
        Assert.AreEqual(GaussDBDbType.Integer, p.GaussDBDbType, "#A:GaussDBDbType");
        Assert.AreEqual(42, p.Value, "#A:Value");

        p.DbType = DbType.DateTime; //assigning a DbType
        Assert.AreEqual(DbType.DateTime, p.DbType, "#B:DbType1");
        Assert.AreEqual(GaussDBDbType.TimestampTz, p.GaussDBDbType, "#B:SqlDbType1");
        p.ResetDbType();
        Assert.AreEqual(DbType.Int32, p.DbType, "#B:DbType2");
        Assert.AreEqual(GaussDBDbType.Integer, p.GaussDBDbType, "#B:SqlDbtype2");

        //Parameter with an assigned GaussDBDbType but no specified value
        p = new GaussDBParameter("foo", GaussDBDbType.Integer);
        p.ResetDbType();
        Assert.AreEqual(DbType.Object, p.DbType, "#C:DbType");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#C:GaussDBDbType");

        p.GaussDBDbType = GaussDBDbType.TimestampTz; //assigning a GaussDBDbType
        Assert.AreEqual(DbType.DateTime, p.DbType, "#D:DbType1");
        Assert.AreEqual(GaussDBDbType.TimestampTz, p.GaussDBDbType, "#D:SqlDbType1");
        p.ResetDbType();
        Assert.AreEqual(DbType.Object, p.DbType, "#D:DbType2");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#D:SqlDbType2");

        p = new GaussDBParameter();
        p.Value = DateTime.MaxValue;
        Assert.AreEqual(DbType.DateTime2, p.DbType, "#E:DbType1");
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#E:SqlDbType1");
        p.Value = null;
        p.ResetDbType();
        Assert.AreEqual(DbType.Object, p.DbType, "#E:DbType2");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#E:SqlDbType2");

        p = new GaussDBParameter("foo", GaussDBDbType.Varchar);
        p.Value = DateTime.MaxValue;
        p.ResetDbType();
        Assert.AreEqual(DbType.DateTime2, p.DbType, "#F:DbType");
        Assert.AreEqual(GaussDBDbType.Timestamp, p.GaussDBDbType, "#F:GaussDBDbType");
        Assert.AreEqual(DateTime.MaxValue, p.Value, "#F:Value");

        p = new GaussDBParameter("foo", GaussDBDbType.Varchar);
        p.Value = DBNull.Value;
        p.ResetDbType();
        Assert.AreEqual(DbType.Object, p.DbType, "#G:DbType");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#G:GaussDBDbType");
        Assert.AreEqual(DBNull.Value, p.Value, "#G:Value");

        p = new GaussDBParameter("foo", GaussDBDbType.Varchar);
        p.Value = null;
        p.ResetDbType();
        Assert.AreEqual(DbType.Object, p.DbType, "#G:DbType");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#G:GaussDBDbType");
        Assert.IsNull(p.Value, "#G:Value");
    }

    [Test]
    public void ParameterName_retains_prefix()
        => Assert.That(new GaussDBParameter("@p", DbType.String).ParameterName, Is.EqualTo("@p"));

    [Test]
    [Ignore("")]
    public void SourceColumn()
    {
        var p = new GaussDBParameter();
        p.SourceColumn = "name";
        Assert.AreEqual(string.Empty, p.ParameterName, "#A:ParameterName");
        Assert.AreEqual("name", p.SourceColumn, "#A:SourceColumn");

        p.SourceColumn = null;
        Assert.AreEqual(string.Empty, p.ParameterName, "#B:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#B:SourceColumn");

        p.SourceColumn = " ";
        Assert.AreEqual(string.Empty, p.ParameterName, "#C:ParameterName");
        Assert.AreEqual(" ", p.SourceColumn, "#C:SourceColumn");

        p.SourceColumn = " name ";
        Assert.AreEqual(string.Empty, p.ParameterName, "#D:ParameterName");
        Assert.AreEqual(" name ", p.SourceColumn, "#D:SourceColumn");

        p.SourceColumn = string.Empty;
        Assert.AreEqual(string.Empty, p.ParameterName, "#E:ParameterName");
        Assert.AreEqual(string.Empty, p.SourceColumn, "#E:SourceColumn");
    }

    [Test]
    public void Bug1011100_GaussDBDbType()
    {
        var p = new GaussDBParameter();
        p.Value = DBNull.Value;
        Assert.AreEqual(DbType.Object, p.DbType, "#A:DbType");
        Assert.AreEqual(GaussDBDbType.Unknown, p.GaussDBDbType, "#A:GaussDBDbType");

        // Now change parameter value.
        // Note that as we didn't explicitly specified a dbtype, the dbtype property should change when
        // the value changes...

        p.Value = 8;

        Assert.AreEqual(DbType.Int32, p.DbType, "#A:DbType");
        Assert.AreEqual(GaussDBDbType.Integer, p.GaussDBDbType, "#A:GaussDBDbType");

        //Assert.AreEqual(3510, p.Value, "#A:Value");
        //p.GaussDBDbType = GaussDBDbType.Varchar;
        //Assert.AreEqual(DbType.String, p.DbType, "#B:DbType");
        //Assert.AreEqual(GaussDBDbType.Varchar, p.GaussDBDbType, "#B:GaussDBDbType");
        //Assert.AreEqual(3510, p.Value, "#B:Value");
    }

    [Test]
    public void GaussDBParameter_Clone()
    {
        var param = new GaussDBParameter();

        param.Value = 5;
        param.Precision = 1;
        param.Scale = 1;
        param.Size = 1;
        param.Direction = ParameterDirection.Input;
        param.IsNullable = true;
        param.ParameterName = "parameterName";
        param.SourceColumn = "source_column";
        param.SourceVersion = DataRowVersion.Current;
        param.GaussDBValue = 5;
        param.SourceColumnNullMapping = false;

        var newParam = param.Clone();

        Assert.AreEqual(param.Value, newParam.Value);
        Assert.AreEqual(param.Precision, newParam.Precision);
        Assert.AreEqual(param.Scale, newParam.Scale);
        Assert.AreEqual(param.Size, newParam.Size);
        Assert.AreEqual(param.Direction, newParam.Direction);
        Assert.AreEqual(param.IsNullable, newParam.IsNullable);
        Assert.AreEqual(param.ParameterName, newParam.ParameterName);
        Assert.AreEqual(param.TrimmedName, newParam.TrimmedName);
        Assert.AreEqual(param.SourceColumn, newParam.SourceColumn);
        Assert.AreEqual(param.SourceVersion, newParam.SourceVersion);
        Assert.AreEqual(param.GaussDBValue, newParam.GaussDBValue);
        Assert.AreEqual(param.SourceColumnNullMapping, newParam.SourceColumnNullMapping);
        Assert.AreEqual(param.GaussDBValue, newParam.GaussDBValue);

    }

    [Test]
    public void Precision_via_interface()
    {
        var parameter = new GaussDBParameter();
        var paramIface = (IDbDataParameter)parameter;

        paramIface.Precision = 42;

        Assert.AreEqual((byte)42, paramIface.Precision);
    }

    [Test]
    public void Precision_via_base_class()
    {
        var parameter = new GaussDBParameter();
        var paramBase = (DbParameter)parameter;

        paramBase.Precision = 42;

        Assert.AreEqual((byte)42, paramBase.Precision);
    }

    [Test]
    public void Scale_via_interface()
    {
        var parameter = new GaussDBParameter();
        var paramIface = (IDbDataParameter)parameter;

        paramIface.Scale = 42;

        Assert.AreEqual((byte)42, paramIface.Scale);
    }

    [Test]
    public void Scale_via_base_class()
    {
        var parameter = new GaussDBParameter();
        var paramBase = (DbParameter)parameter;

        paramBase.Scale = 42;

        Assert.AreEqual((byte)42, paramBase.Scale);
    }

    [Test]
    public void Null_value_throws()
    {
        using var connection = OpenConnection();
        using var command = new GaussDBCommand("SELECT @p", connection)
        {
            Parameters = { new GaussDBParameter("p", null) }
        };

        Assert.That(() => command.ExecuteReader(), Throws.InvalidOperationException);
    }

    [Test]
    public void Null_value_with_nullable_type()
    {
        using var connection = OpenConnection();
        using var command = new GaussDBCommand("SELECT @p", connection)
        {
            Parameters = { new GaussDBParameter<int?>("p", null) }
        };
        using var reader = command.ExecuteReader();

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<int?>(0), Is.Null);
    }

    [Test]
    public void DBNull_reuses_type_info([Values]bool generic)
    {
        var param = generic ? new GaussDBParameter<object> { Value = "value" } : new GaussDBParameter { Value = "value" };
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var typeInfo, out _, out _);
        Assert.That(typeInfo, Is.Not.Null);

        // Make sure we don't reset the type info when setting DBNull.
        param.Value = DBNull.Value;
        param.GetResolutionInfo(out var secondTypeInfo, out _, out _);
        Assert.That(secondTypeInfo, Is.SameAs(typeInfo));

        // Make sure we don't resolve a different type info either.
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var thirdTypeInfo, out _, out _);
        Assert.That(thirdTypeInfo, Is.SameAs(secondTypeInfo));
    }

    [Test]
    public void DBNull_followed_by_non_null_reresolves([Values]bool generic)
    {
        var param = generic ? new GaussDBParameter<object> { Value = DBNull.Value } : new GaussDBParameter { Value = DBNull.Value };
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var typeInfo, out _, out var pgTypeId);
        Assert.That(typeInfo, Is.Not.Null);
        Assert.That(pgTypeId.IsUnspecified, Is.True);

        param.Value = "value";
        param.GetResolutionInfo(out var secondTypeInfo, out _, out _);
        Assert.That(secondTypeInfo, Is.Null);

        // Make sure we don't resolve the same type info either.
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var thirdTypeInfo, out _, out _);
        Assert.That(thirdTypeInfo, Is.Not.SameAs(typeInfo));
    }

    [Test]
    public void Changing_value_type_reresolves([Values]bool generic)
    {
        var param = generic ? new GaussDBParameter<object> { Value = "value" } : new GaussDBParameter { Value = "value" };
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var typeInfo, out _, out _);
        Assert.That(typeInfo, Is.Not.Null);

        param.Value = 1;
        param.GetResolutionInfo(out var secondTypeInfo, out _, out _);
        Assert.That(secondTypeInfo, Is.Null);

        // Make sure we don't resolve a different type info either.
        param.ResolveTypeInfo(DataSource.SerializerOptions);
        param.GetResolutionInfo(out var thirdTypeInfo, out _, out _);
        Assert.That(thirdTypeInfo, Is.Not.SameAs(typeInfo));
    }

#if NeedsPorting
    [Test]
    [Category ("NotWorking")]
    public void InferType_Char()
    {
        Char value = 'X';

        String string_value = "X";

        GaussDBParameter p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#A:GaussDBDbType");
        Assert.AreEqual (DbType.String, p.DbType, "#A:DbType");
        Assert.AreEqual (string_value, p.Value, "#A:Value");

        p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#B:Value1");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#B:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#B:Value2");

        p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#C:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#C:DbType");
        Assert.AreEqual (string_value, p.Value, "#C:Value2");

        p = new GaussDBParameter ("name", value);
        Assert.AreEqual (value, p.Value, "#D:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#D:DbType");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#D:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#D:Value2");

        p = new GaussDBParameter ("name", 5);
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#E:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#E:DbType");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#E:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#E:Value2");

        p = new GaussDBParameter ("name", GaussDBDbType.Text);
        p.Value = value;
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#F:GaussDBDbType");
        Assert.AreEqual (value, p.Value, "#F:Value");
    }

    [Test]
    [Category ("NotWorking")]
    public void InferType_CharArray()
    {
        Char[] value = new Char[] { 'A', 'X' };

        String string_value = "AX";

        GaussDBParameter p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#A:Value1");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#A:GaussDBDbType");
        Assert.AreEqual (DbType.String, p.DbType, "#A:DbType");
        Assert.AreEqual (string_value, p.Value, "#A:Value2");

        p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#B:Value1");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#B:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#B:Value2");

        p = new GaussDBParameter ();
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#C:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#C:DbType");
        Assert.AreEqual (string_value, p.Value, "#C:Value2");

        p = new GaussDBParameter ("name", value);
        Assert.AreEqual (value, p.Value, "#D:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#D:DbType");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#D:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#D:Value2");

        p = new GaussDBParameter ("name", 5);
        p.Value = value;
        Assert.AreEqual (value, p.Value, "#E:Value1");
        Assert.AreEqual (DbType.String, p.DbType, "#E:DbType");
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#E:GaussDBDbType");
        Assert.AreEqual (string_value, p.Value, "#E:Value2");

        p = new GaussDBParameter ("name", GaussDBDbType.Text);
        p.Value = value;
        Assert.AreEqual (GaussDBDbType.Text, p.GaussDBDbType, "#F:GaussDBDbType");
        Assert.AreEqual (value, p.Value, "#F:Value");
    }

    [Test]
    public void InferType_Object()
    {
        Object value = new Object();

        GaussDBParameter param = new GaussDBParameter();
        param.Value = value;
        Assert.AreEqual(GaussDBDbType.Variant, param.GaussDBDbType, "#1");
        Assert.AreEqual(DbType.Object, param.DbType, "#2");
    }

    [Test]
    public void LocaleId ()
    {
        GaussDBParameter parameter = new GaussDBParameter ();
        Assert.AreEqual (0, parameter.LocaleId, "#1");
        parameter.LocaleId = 15;
        Assert.AreEqual(15, parameter.LocaleId, "#2");
    }
#endif

    [OneTimeSetUp]
    public async Task Bootstrap()
    {
        // Bootstrap datasource.
        await using (var _ = await OpenConnectionAsync()) {}
    }
}
