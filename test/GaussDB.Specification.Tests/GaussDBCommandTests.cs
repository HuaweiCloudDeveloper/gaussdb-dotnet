using AdoNet.Specification.Tests;

namespace GaussDB.Specification.Tests;
//todo: 均因为不支持DISCARD命令，导致测试失败，测试用例暂未找到源码
/*
public sealed class GaussDBCommandTests(GaussDBDbFactoryFixture fixture) : CommandTestBase<GaussDBDbFactoryFixture>(fixture)
{
    // PostgreSQL only supports a single transaction on a given connection at a given time. As a result,
    // GaussDB completely ignores DbCommand.Transaction.
    public override void ExecuteReader_throws_when_transaction_required() {}
    public override void ExecuteReader_throws_when_transaction_mismatched() {}
}
*/
