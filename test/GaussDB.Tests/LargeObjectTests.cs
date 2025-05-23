using System.Linq;
using System.Text;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests;

#pragma warning disable CS0618 // Large object support is obsolete
//todo: GaussDB 不支持Large object ，参考：https://bbs.huaweicloud.com/forum/thread-0211178941356334146-1-1.html
/*public class LargeObjectTests : TestBase
{
    [Test]
    public void Test()
    {
        using var conn = OpenConnection();
        using var transaction = conn.BeginTransaction();
        var manager = new GaussDBLargeObjectManager(conn);
        var oid = manager.Create();
        using (var stream = manager.OpenReadWrite(oid))
        {
            var buf = Encoding.UTF8.GetBytes("Hello");
            stream.Write(buf, 0, buf.Length);
            stream.Seek(0, System.IO.SeekOrigin.Begin);
            var buf2 = new byte[buf.Length];
            stream.ReadExactly(buf2, 0, buf2.Length);
            Assert.That(buf.SequenceEqual(buf2));

            Assert.AreEqual(5, stream.Position);

            Assert.AreEqual(5, stream.Length);

            stream.Seek(-1, System.IO.SeekOrigin.Current);
            Assert.AreEqual((int)'o', stream.ReadByte());

            manager.MaxTransferBlockSize = 3;

            stream.Write(buf, 0, buf.Length);
            stream.Seek(-5, System.IO.SeekOrigin.End);
            var buf3 = new byte[100];
            Assert.AreEqual(5, stream.Read(buf3, 0, 100));
            Assert.That(buf.SequenceEqual(buf3.Take(5)));

            stream.SetLength(43);
            Assert.AreEqual(43, stream.Length);
        }

        manager.Unlink(oid);

        transaction.Rollback();
    }
}*/
