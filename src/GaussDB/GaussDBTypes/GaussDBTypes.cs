using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

#pragma warning disable 1591

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDBTypes;

/// <summary>
/// Represents a PostgreSQL point type.
/// </summary>
/// <remarks>
/// See https://www.postgresql.org/docs/current/static/datatype-geometric.html
/// </remarks>
public struct GaussDBPoint(double x, double y) : IEquatable<GaussDBPoint>
{
    public double X { get; set; } = x;
    public double Y { get; set; } = y;

    // ReSharper disable CompareOfFloatsByEqualityOperator
    public bool Equals(GaussDBPoint other) => X == other.X && Y == other.Y;
    // ReSharper restore CompareOfFloatsByEqualityOperator

    public override bool Equals(object? obj)
        => obj is GaussDBPoint point && Equals(point);

    public static bool operator ==(GaussDBPoint x, GaussDBPoint y) => x.Equals(y);

    public static bool operator !=(GaussDBPoint x, GaussDBPoint y) => !(x == y);

    public override int GetHashCode()
        => HashCode.Combine(X, Y);

    public override string ToString()
        => string.Format(CultureInfo.InvariantCulture, "({0},{1})", X, Y);
}

/// <summary>
/// Represents a PostgreSQL line type.
/// </summary>
/// <remarks>
/// See https://www.postgresql.org/docs/current/static/datatype-geometric.html
/// </remarks>
public struct GaussDBLine(double a, double b, double c) : IEquatable<GaussDBLine>
{
    public double A { get; set; } = a;
    public double B { get; set; } = b;
    public double C { get; set; } = c;

    public override string ToString()
        => string.Format(CultureInfo.InvariantCulture, "{{{0},{1},{2}}}", A, B, C);

    public override int GetHashCode()
        => HashCode.Combine(A, B, C);

    public bool Equals(GaussDBLine other)
        => A == other.A && B == other.B && C == other.C;

    public override bool Equals(object? obj)
        => obj is GaussDBLine line && Equals(line);

    public static bool operator ==(GaussDBLine x, GaussDBLine y) => x.Equals(y);
    public static bool operator !=(GaussDBLine x, GaussDBLine y) => !(x == y);
}

/// <summary>
/// Represents a PostgreSQL Line Segment type.
/// </summary>
public struct GaussDBLSeg : IEquatable<GaussDBLSeg>
{
    public GaussDBPoint Start { get; set; }
    public GaussDBPoint End { get; set; }

    public GaussDBLSeg(GaussDBPoint start, GaussDBPoint end)
        : this()
    {
        Start = start;
        End = end;
    }

    public GaussDBLSeg(double startx, double starty, double endx, double endy) : this()
    {
        Start = new GaussDBPoint(startx, starty);
        End = new GaussDBPoint(endx,   endy);
    }

    public override string ToString()
        => string.Format(CultureInfo.InvariantCulture, "[{0},{1}]", Start, End);

    public override int GetHashCode()
        => HashCode.Combine(Start.X, Start.Y, End.X, End.Y);

    public bool Equals(GaussDBLSeg other)
        => Start == other.Start && End == other.End;

    public override bool Equals(object? obj)
        => obj is GaussDBLSeg seg && Equals(seg);

    public static bool operator ==(GaussDBLSeg x, GaussDBLSeg y) => x.Equals(y);
    public static bool operator !=(GaussDBLSeg x, GaussDBLSeg y) => !(x == y);
}

/// <summary>
/// Represents a PostgreSQL box type.
/// </summary>
/// <remarks>
/// See https://www.postgresql.org/docs/current/static/datatype-geometric.html
/// </remarks>
public struct GaussDBBox : IEquatable<GaussDBBox>
{
    GaussDBPoint _upperRight;
    public GaussDBPoint UpperRight
    {
        get => _upperRight;
        set
        {
            _upperRight = value;
            NormalizeBox();
        }
    }

    GaussDBPoint _lowerLeft;
    public GaussDBPoint LowerLeft
    {
        get => _lowerLeft;
        set
        {
            _lowerLeft = value;
            NormalizeBox();
        }
    }

    public GaussDBBox(GaussDBPoint upperRight, GaussDBPoint lowerLeft) : this()
    {
        _upperRight = upperRight;
        _lowerLeft = lowerLeft;
        NormalizeBox();
    }

    public GaussDBBox(double top, double right, double bottom, double left)
        : this(new GaussDBPoint(right, top), new GaussDBPoint(left, bottom)) { }

    public double Left => LowerLeft.X;
    public double Right => UpperRight.X;
    public double Bottom => LowerLeft.Y;
    public double Top => UpperRight.Y;
    public double Width => Right - Left;
    public double Height => Top - Bottom;

    public bool IsEmpty => Width == 0 || Height == 0;

    public bool Equals(GaussDBBox other)
        => UpperRight == other.UpperRight && LowerLeft == other.LowerLeft;

    public override bool Equals(object? obj)
        => obj is GaussDBBox box && Equals(box);

    public static bool operator ==(GaussDBBox x, GaussDBBox y) => x.Equals(y);
    public static bool operator !=(GaussDBBox x, GaussDBBox y) => !(x == y);
    public override string ToString()
        => string.Format(CultureInfo.InvariantCulture, "{0},{1}", UpperRight, LowerLeft);

    public override int GetHashCode()
        => HashCode.Combine(Top, Right, Bottom, LowerLeft);

    // Swaps corners for isomorphic boxes, to mirror postgres behavior.
    // See: https://github.com/postgres/postgres/blob/af2324fabf0020e464b0268be9ef03e8f46ed84b/src/backend/utils/adt/geo_ops.c#L435-L447
    void NormalizeBox()
    {
        if (_upperRight.X < _lowerLeft.X)
            (_upperRight.X, _lowerLeft.X) = (_lowerLeft.X, _upperRight.X);

        if (_upperRight.Y < _lowerLeft.Y)
            (_upperRight.Y, _lowerLeft.Y) = (_lowerLeft.Y, _upperRight.Y);
    }
}

/// <summary>
/// Represents a PostgreSQL Path type.
/// </summary>
public struct GaussDBPath : IList<GaussDBPoint>, IEquatable<GaussDBPath>
{
    List<GaussDBPoint> _points;

    List<GaussDBPoint> Points => _points ??= [];

    public bool Open { get; set; }

    public GaussDBPath()
        => _points = [];

    public GaussDBPath(IEnumerable<GaussDBPoint> points, bool open)
    {
        _points = [..points];
        Open = open;
    }

    public GaussDBPath(IEnumerable<GaussDBPoint> points) : this(points, false) {}
    public GaussDBPath(params GaussDBPoint[] points) : this(points, false) {}

    public GaussDBPath(bool open) : this()
    {
        _points = [];
        Open = open;
    }

    public GaussDBPath(int capacity, bool open) : this()
    {
        _points = new List<GaussDBPoint>(capacity);
        Open = open;
    }

    public GaussDBPath(int capacity) : this(capacity, false) {}

    public GaussDBPoint this[int index]
    {
        get => Points[index];
        set => Points[index] = value;
    }

    public int Capacity => Points.Capacity;
    public int Count => _points?.Count ?? 0;
    public bool IsReadOnly => false;

    public int IndexOf(GaussDBPoint item) => Points.IndexOf(item);
    public void Insert(int index, GaussDBPoint item) => Points.Insert(index, item);
    public void RemoveAt(int index) => Points.RemoveAt(index);
    public void Add(GaussDBPoint item) => Points.Add(item);
    public void Clear() => Points.Clear();
    public bool Contains(GaussDBPoint item) => Points.Contains(item);
    public void CopyTo(GaussDBPoint[] array, int arrayIndex) => Points.CopyTo(array, arrayIndex);
    public bool Remove(GaussDBPoint item) => Points.Remove(item);
    public IEnumerator<GaussDBPoint> GetEnumerator() => Points.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public bool Equals(GaussDBPath other)
    {
        if (Open != other.Open || Count != other.Count)
            return false;
        if (ReferenceEquals(_points, other._points))//Short cut for shallow copies.
            return true;
        for (var i = 0; i != Count; ++i)
            if (this[i] != other[i])
                return false;
        return true;
    }

    public override bool Equals(object? obj)
        => obj is GaussDBPath path && Equals(path);

    public static bool operator ==(GaussDBPath x, GaussDBPath y) => x.Equals(y);
    public static bool operator !=(GaussDBPath x, GaussDBPath y) => !(x == y);

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(Open);

        foreach (var point in this)
        {
            hashCode.Add(point.X);
            hashCode.Add(point.Y);
        }

        return hashCode.ToHashCode();
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(Open ? '[' : '(');
        int i;
        for (i = 0; i < Count; i++)
        {
            var p = _points[i];
            sb.AppendFormat(CultureInfo.InvariantCulture, "({0},{1})", p.X, p.Y);
            if (i < _points.Count - 1)
                sb.Append(',');
        }
        sb.Append(Open ? ']' : ')');
        return sb.ToString();
    }
}

/// <summary>
/// Represents a PostgreSQL Polygon type.
/// </summary>
public struct GaussDBPolygon : IList<GaussDBPoint>, IEquatable<GaussDBPolygon>
{
    List<GaussDBPoint> _points;

    List<GaussDBPoint> Points => _points ??= [];

    public GaussDBPolygon()
        => _points = [];

    public GaussDBPolygon(IEnumerable<GaussDBPoint> points)
        => _points = [..points];

    public GaussDBPolygon(params GaussDBPoint[] points) : this((IEnumerable<GaussDBPoint>) points) {}

    public GaussDBPolygon(int capacity)
        => _points = new List<GaussDBPoint>(capacity);

    public GaussDBPoint this[int index]
    {
        get => Points[index];
        set => Points[index] = value;
    }

    public int Capacity => Points.Capacity;
    public int Count => _points?.Count ?? 0;
    public bool IsReadOnly => false;

    public int IndexOf(GaussDBPoint item) => Points.IndexOf(item);
    public void Insert(int index, GaussDBPoint item) => Points.Insert(index, item);
    public void RemoveAt(int index) => Points.RemoveAt(index);
    public void Add(GaussDBPoint item) => Points.Add(item);
    public void Clear() => Points.Clear();
    public bool Contains(GaussDBPoint item) => Points.Contains(item);
    public void CopyTo(GaussDBPoint[] array, int arrayIndex) => Points.CopyTo(array, arrayIndex);
    public bool Remove(GaussDBPoint item) => Points.Remove(item);
    public IEnumerator<GaussDBPoint> GetEnumerator() => Points.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public bool Equals(GaussDBPolygon other)
    {
        if (Count != other.Count)
            return false;
        if (ReferenceEquals(_points, other._points))
            return true;
        for (var i = 0; i != Count; ++i)
            if (this[i] != other[i])
                return false;
        return true;
    }

    public override bool Equals(object? obj)
        => obj is GaussDBPolygon polygon && Equals(polygon);

    public static bool operator ==(GaussDBPolygon x, GaussDBPolygon y) => x.Equals(y);
    public static bool operator !=(GaussDBPolygon x, GaussDBPolygon y) => !(x == y);

    public override int GetHashCode()
    {
        var hashCode = new HashCode();

        foreach (var point in this)
        {
            hashCode.Add(point.X);
            hashCode.Add(point.Y);
        }

        return hashCode.ToHashCode();
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append('(');
        int i;
        for (i = 0; i < Count; i++)
        {
            var p = _points[i];
            sb.AppendFormat(CultureInfo.InvariantCulture, "({0},{1})", p.X, p.Y);
            if (i < _points.Count - 1) {
                sb.Append(",");
            }
        }
        sb.Append(')');
        return sb.ToString();
    }
}

/// <summary>
/// Represents a PostgreSQL Circle type.
/// </summary>
public struct GaussDBCircle(double x, double y, double radius) : IEquatable<GaussDBCircle>
{
    public double X { get; set; } = x;
    public double Y { get; set; } = y;
    public double Radius { get; set; } = radius;

    public GaussDBCircle(GaussDBPoint center, double radius)
        : this(center.X, center.Y, radius)
    {
    }

    public GaussDBPoint Center
    {
        get => new(X, Y);
        set => (X, Y) = (value.X, value.Y);
    }

    // ReSharper disable CompareOfFloatsByEqualityOperator
    public bool Equals(GaussDBCircle other)
        => X == other.X && Y == other.Y && Radius == other.Radius;
    // ReSharper restore CompareOfFloatsByEqualityOperator

    public override bool Equals(object? obj)
        => obj is GaussDBCircle circle && Equals(circle);

    public override string ToString()
        => string.Format(CultureInfo.InvariantCulture, "<({0},{1}),{2}>", X, Y, Radius);

    public static bool operator ==(GaussDBCircle x, GaussDBCircle y) => x.Equals(y);
    public static bool operator !=(GaussDBCircle x, GaussDBCircle y) => !(x == y);

    public override int GetHashCode()
        => HashCode.Combine(X, Y, Radius);
}

/// <summary>
/// Represents a PostgreSQL inet type, which is a combination of an IPAddress and a subnet mask.
/// </summary>
/// <remarks>
/// https://www.postgresql.org/docs/current/static/datatype-net-types.html
/// </remarks>
public readonly record struct GaussDBInet
{
    public IPAddress Address { get; }
    public byte Netmask { get; }

    public GaussDBInet(IPAddress address, byte netmask)
    {
        CheckAddressFamily(address);
        Address = address;
        Netmask = netmask;
    }

    public GaussDBInet(IPAddress address)
        : this(address, (byte)(address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128))
    {
    }

    public GaussDBInet(string addr)
    {
        switch (addr.Split('/'))
        {
        case { Length: 2 } segments:
            (Address, Netmask) = (IPAddress.Parse(segments[0]), byte.Parse(segments[1]));
            break;
        case { Length: 1 } segments:
            var ipAddr = IPAddress.Parse(segments[0]);
            CheckAddressFamily(ipAddr);
            (Address, Netmask) = (
                ipAddr,
                ipAddr.AddressFamily == AddressFamily.InterNetworkV6 ? (byte)128 : (byte)32);
            break;
        default:
            throw new FormatException("Invalid number of parts in CIDR specification");
        }
    }

    public override string ToString()
        => (Address?.AddressFamily == AddressFamily.InterNetwork && Netmask == 32) ||
           (Address?.AddressFamily == AddressFamily.InterNetworkV6 && Netmask == 128)
            ? Address.ToString()
            : $"{Address}/{Netmask}";

    public static explicit operator IPAddress(GaussDBInet inet)
        => inet.Address;

    public static implicit operator GaussDBInet(IPAddress ip)
        => new(ip);

    public void Deconstruct(out IPAddress address, out byte netmask)
    {
        address = Address;
        netmask = Netmask;
    }

    static void CheckAddressFamily(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork && address.AddressFamily != AddressFamily.InterNetworkV6)
            throw new ArgumentException("Only IPAddress of InterNetwork or InterNetworkV6 address families are accepted", nameof(address));
    }
}

/// <summary>
/// Represents a PostgreSQL cidr type.
/// </summary>
/// <remarks>
/// https://www.postgresql.org/docs/current/static/datatype-net-types.html
/// </remarks>
[Obsolete("Use .NET IPNetwork instead of GaussDBCidr to map to PostgreSQL cidr")]
public readonly record struct GaussDBCidr
{
    public IPAddress Address { get; }
    public byte Netmask { get; }

    public GaussDBCidr(IPAddress address, byte netmask)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork && address.AddressFamily != AddressFamily.InterNetworkV6)
            throw new ArgumentException("Only IPAddress of InterNetwork or InterNetworkV6 address families are accepted", nameof(address));

        Address = address;
        Netmask = netmask;
    }

    public GaussDBCidr(string addr)
        => (Address, Netmask) = addr.Split('/') switch
        {
            { Length: 2 } segments => (IPAddress.Parse(segments[0]), byte.Parse(segments[1])),
            { Length: 1 } => throw new FormatException("Missing netmask"),
            _ => throw new FormatException("Invalid number of parts in CIDR specification")
        };

    public static implicit operator GaussDBInet(GaussDBCidr cidr)
        => new(cidr.Address, cidr.Netmask);

    public static explicit operator IPAddress(GaussDBCidr cidr)
        => cidr.Address;

    public override string ToString()
        => $"{Address}/{Netmask}";

    public void Deconstruct(out IPAddress address, out byte netmask)
    {
        address = Address;
        netmask = Netmask;
    }
}

/// <summary>
/// Represents a PostgreSQL tid value
/// </summary>
/// <remarks>
/// https://www.postgresql.org/docs/current/static/datatype-oid.html
/// </remarks>
public readonly struct GaussDBTid(uint blockNumber, ushort offsetNumber) : IEquatable<GaussDBTid>
{
    /// <summary>
    /// Block number
    /// </summary>
    public uint BlockNumber { get; } = blockNumber;

    /// <summary>
    /// Tuple index within block
    /// </summary>
    public ushort OffsetNumber { get; } = offsetNumber;

    public bool Equals(GaussDBTid other)
        => BlockNumber == other.BlockNumber && OffsetNumber == other.OffsetNumber;

    public override bool Equals(object? o)
        => o is GaussDBTid tid && Equals(tid);

    public override int GetHashCode() => (int)BlockNumber ^ OffsetNumber;
    public static bool operator ==(GaussDBTid left, GaussDBTid right) => left.Equals(right);
    public static bool operator !=(GaussDBTid left, GaussDBTid right) => !(left == right);
    public override string ToString() => $"({BlockNumber},{OffsetNumber})";
}

#pragma warning restore 1591
