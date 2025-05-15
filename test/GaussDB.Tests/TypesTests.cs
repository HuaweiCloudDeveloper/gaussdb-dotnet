using System;
using System.Net;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests;

/// <summary>
/// Tests GaussDBTypes.* independent of a database
/// </summary>
public class TypesTests
{
#pragma warning disable CS0618 // {GaussDBTsVector,GaussDBTsQuery}.Parse are obsolete
    [Test]
    public void TsVector()
    {
        GaussDBTsVector vec;

        vec = GaussDBTsVector.Parse("a");
        Assert.AreEqual("'a'", vec.ToString());

        vec = GaussDBTsVector.Parse("a ");
        Assert.AreEqual("'a'", vec.ToString());

        vec = GaussDBTsVector.Parse("a:1A");
        Assert.AreEqual("'a':1A", vec.ToString());

        vec = GaussDBTsVector.Parse(@"\abc\def:1a ");
        Assert.AreEqual("'abcdef':1A", vec.ToString());

        vec = GaussDBTsVector.Parse(@"abc:3A 'abc' abc:4B 'hello''yo' 'meh\'\\':5");
        Assert.AreEqual(@"'abc':3A,4B 'hello''yo' 'meh''\\':5", vec.ToString());

        vec = GaussDBTsVector.Parse(" a:12345C  a:24D a:25B b c d 1 2 a:25A,26B,27,28");
        Assert.AreEqual("'1' '2' 'a':24,25A,26B,27,28,12345C 'b' 'c' 'd'", vec.ToString());
    }

    [Test]
    public void TsQuery()
    {
        GaussDBTsQuery query;

        query = new GaussDBTsQueryLexeme("a", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B);
        query = new GaussDBTsQueryOr(query, query);
        query = new GaussDBTsQueryOr(query, query);

        var str = query.ToString();

        query = GaussDBTsQuery.Parse("a & b | c");
        Assert.AreEqual("'a' & 'b' | 'c'", query.ToString());

        query = GaussDBTsQuery.Parse("'a''':*ab&d:d&!c");
        Assert.AreEqual("'a''':*AB & 'd':D & !'c'", query.ToString());

        query = GaussDBTsQuery.Parse("(a & !(c | d)) & (!!a&b) | c | d | e");
        Assert.AreEqual("( ( 'a' & !( 'c' | 'd' ) & !( !'a' ) & 'b' | 'c' ) | 'd' ) | 'e'", query.ToString());
        Assert.AreEqual(query.ToString(), GaussDBTsQuery.Parse(query.ToString()).ToString());

        query = GaussDBTsQuery.Parse("(((a:*)))");
        Assert.AreEqual("'a':*", query.ToString());

        query = GaussDBTsQuery.Parse(@"'a\\b''cde'");
        Assert.AreEqual(@"a\b'cde", ((GaussDBTsQueryLexeme)query).Text);
        Assert.AreEqual(@"'a\\b''cde'", query.ToString());

        query = GaussDBTsQuery.Parse(@"a <-> b");
        Assert.AreEqual("'a' <-> 'b'", query.ToString());

        query = GaussDBTsQuery.Parse("((a & b) <5> c) <-> !d <0> e");
        Assert.AreEqual("( ( 'a' & 'b' <5> 'c' ) <-> !'d' ) <0> 'e'", query.ToString());

        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("a b c & &"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("&"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("|"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("!"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("("));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse(")"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("()"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("<"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("<-"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("<->"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("a <->"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("<>"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("a <a> b"));
        Assert.Throws(typeof(FormatException), () => GaussDBTsQuery.Parse("a <-1> b"));
    }
#pragma warning restore CS0618 // {GaussDBTsVector,GaussDBTsQuery}.Parse are obsolete

    [Test]
    public void TsQueryEquatibility()
    {
        //Debugger.Launch();
        AreEqual(
            new GaussDBTsQueryLexeme("lexeme"),
            new GaussDBTsQueryLexeme("lexeme"));

        AreEqual(
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B),
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B));

        AreEqual(
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B, true),
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B, true));

        AreEqual(
            new GaussDBTsQueryNot(new GaussDBTsQueryLexeme("not")),
            new GaussDBTsQueryNot(new GaussDBTsQueryLexeme("not")));

        AreEqual(
            new GaussDBTsQueryAnd(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")),
            new GaussDBTsQueryAnd(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")));

        AreEqual(
            new GaussDBTsQueryOr(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")),
            new GaussDBTsQueryOr(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")));

        AreEqual(
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 0, new GaussDBTsQueryLexeme("right")),
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 0, new GaussDBTsQueryLexeme("right")));

        AreEqual(
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 1, new GaussDBTsQueryLexeme("right")),
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 1, new GaussDBTsQueryLexeme("right")));

        AreEqual(
            new GaussDBTsQueryEmpty(),
            new GaussDBTsQueryEmpty());

        AreNotEqual(
            new GaussDBTsQueryLexeme("lexeme a"),
            new GaussDBTsQueryLexeme("lexeme b"));

        AreNotEqual(
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.D),
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B));

        AreNotEqual(
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B, true),
            new GaussDBTsQueryLexeme("lexeme", GaussDBTsQueryLexeme.Weight.A | GaussDBTsQueryLexeme.Weight.B, false));

        AreNotEqual(
            new GaussDBTsQueryNot(new GaussDBTsQueryLexeme("not")),
            new GaussDBTsQueryNot(new GaussDBTsQueryLexeme("ton")));

        AreNotEqual(
            new GaussDBTsQueryAnd(new GaussDBTsQueryLexeme("right"), new GaussDBTsQueryLexeme("left")),
            new GaussDBTsQueryAnd(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")));

        AreNotEqual(
            new GaussDBTsQueryOr(new GaussDBTsQueryLexeme("right"), new GaussDBTsQueryLexeme("left")),
            new GaussDBTsQueryOr(new GaussDBTsQueryLexeme("left"), new GaussDBTsQueryLexeme("right")));

        AreNotEqual(
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("right"), 0, new GaussDBTsQueryLexeme("left")),
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 0, new GaussDBTsQueryLexeme("right")));

        AreNotEqual(
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 0, new GaussDBTsQueryLexeme("right")),
            new GaussDBTsQueryFollowedBy(new GaussDBTsQueryLexeme("left"), 1, new GaussDBTsQueryLexeme("right")));

        void AreEqual(GaussDBTsQuery left, GaussDBTsQuery right)
        {
            Assert.True(left == right);
            Assert.False(left != right);
            Assert.AreEqual(left, right);
            Assert.AreEqual(left.GetHashCode(), right.GetHashCode());
        }

        void AreNotEqual(GaussDBTsQuery left, GaussDBTsQuery right)
        {
            Assert.False(left == right);
            Assert.True(left != right);
            Assert.AreNotEqual(left, right);
            Assert.AreNotEqual(left.GetHashCode(), right.GetHashCode());
        }
    }

#pragma warning disable CS0618 // {GaussDBTsVector,GaussDBTsQuery}.Parse are obsolete
    [Test]
    public void TsQueryOperatorPrecedence()
    {
        var query = GaussDBTsQuery.Parse("!a <-> b & c | d & e");
        var expectedGrouping = GaussDBTsQuery.Parse("((!(a) <-> b) & c) | (d & e)");
        Assert.AreEqual(expectedGrouping.ToString(), query.ToString());
    }
#pragma warning restore CS0618 // {GaussDBTsVector,GaussDBTsQuery}.Parse are obsolete

    [Test]
    public void GaussDBPath_empty()
        => Assert.That(new GaussDBPath { new(1, 2) }, Is.EqualTo(new GaussDBPath(new GaussDBPoint(1, 2))));

    [Test]
    public void GaussDBPolygon_empty()
        => Assert.That(new GaussDBPolygon { new(1, 2) }, Is.EqualTo(new GaussDBPolygon(new GaussDBPoint(1, 2))));

    [Test]
    public void GaussDBPath_default()
    {
        GaussDBPath defaultPath = default;
        Assert.IsFalse(defaultPath.Equals([new(1, 2)]));
    }

    [Test]
    public void GaussDBPolygon_default()
    {
        GaussDBPolygon defaultPolygon = default;
        Assert.IsFalse(defaultPolygon.Equals([new(1, 2)]));
    }

    [Test]
    public void Bug1011018()
    {
        var p = new GaussDBParameter();
        p.GaussDBDbType = GaussDBDbType.Time;
        p.Value = DateTime.Now;
        var o = p.Value;
    }

    [Test]
    [IssueLink("https://github.com/npgsql/npgsql/issues/750")]
    public void GaussDBInet()
    {
        var v = new GaussDBInet(IPAddress.Parse("2001:1db8:85a3:1142:1000:8a2e:1370:7334"), 32);
        Assert.That(v.ToString(), Is.EqualTo("2001:1db8:85a3:1142:1000:8a2e:1370:7334/32"));
    }

    [Test]
    public void GaussDBInet_parse_ipv4()
    {
        var ipv4 = new GaussDBInet("192.168.1.1/8");
        Assert.That(ipv4.Address, Is.EqualTo(IPAddress.Parse("192.168.1.1")));
        Assert.That(ipv4.Netmask, Is.EqualTo(8));

        ipv4 = new GaussDBInet("192.168.1.1/32");
        Assert.That(ipv4.Address, Is.EqualTo(IPAddress.Parse("192.168.1.1")));
        Assert.That(ipv4.Netmask, Is.EqualTo(32));
    }

    [Test]
    [IssueLink("https://github.com/npgsql/npgsql/issues/5638")]
    public void GaussDBInet_parse_ipv6()
    {
        var ipv6 = new GaussDBInet("2001:0000:130F:0000:0000:09C0:876A:130B/32");
        Assert.That(ipv6.Address, Is.EqualTo(IPAddress.Parse("2001:0000:130F:0000:0000:09C0:876A:130B")));
        Assert.That(ipv6.Netmask, Is.EqualTo(32));

        ipv6 = new GaussDBInet("2001:0000:130F:0000:0000:09C0:876A:130B");
        Assert.That(ipv6.Address, Is.EqualTo(IPAddress.Parse("2001:0000:130F:0000:0000:09C0:876A:130B")));
        Assert.That(ipv6.Netmask, Is.EqualTo(128));
    }

    [Test]
    public void GaussDBInet_ToString_ipv4()
    {
        Assert.That(new GaussDBInet("192.168.1.1/8").ToString(), Is.EqualTo("192.168.1.1/8"));
        Assert.That(new GaussDBInet("192.168.1.1/32").ToString(), Is.EqualTo("192.168.1.1"));
    }

    [Test]
    public void GaussDBInet_ToString_ipv6()
    {
        Assert.That(new GaussDBInet("2001:0:130f::9c0:876a:130b/32").ToString(), Is.EqualTo("2001:0:130f::9c0:876a:130b/32"));
        Assert.That(new GaussDBInet("2001:0:130f::9c0:876a:130b/128").ToString(), Is.EqualTo("2001:0:130f::9c0:876a:130b"));
    }
}
