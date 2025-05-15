using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;
using static GaussDBTypes.GaussDBTsQuery.NodeKind;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB.Internal.Converters;

sealed class TsQueryConverter<T>(Encoding encoding) : PgStreamingConverter<T>
    where T : GaussDBTsQuery
{
    public override T Read(PgReader reader)
        => (T)Read(async: false, reader, CancellationToken.None).GetAwaiter().GetResult();

    public override async ValueTask<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => (T)await Read(async: true, reader, cancellationToken).ConfigureAwait(false);

    async ValueTask<GaussDBTsQuery> Read(bool async, PgReader reader, CancellationToken cancellationToken)
    {
        if (reader.ShouldBuffer(sizeof(int)))
            await reader.Buffer(async, sizeof(int), cancellationToken).ConfigureAwait(false);
        var numTokens = reader.ReadInt32();
        if (numTokens == 0)
            return new GaussDBTsQueryEmpty();

        GaussDBTsQuery? value = null;
        var nodes = new Stack<(GaussDBTsQuery Node, int Location)>();

        for (var i = 0; i < numTokens; i++)
        {
            if (reader.ShouldBuffer(sizeof(byte)))
                await reader.Buffer(async, sizeof(byte), cancellationToken).ConfigureAwait(false);

            switch (reader.ReadByte())
            {
            case 1: // lexeme
                if (reader.ShouldBuffer(sizeof(byte) + sizeof(byte)))
                    await reader.Buffer(async, sizeof(byte) + sizeof(byte), cancellationToken).ConfigureAwait(false);
                var weight = (GaussDBTsQueryLexeme.Weight)reader.ReadByte();
                var prefix = reader.ReadByte() != 0;

                var str = async
                    ? await reader.ReadNullTerminatedStringAsync(encoding, cancellationToken).ConfigureAwait(false)
                    : reader.ReadNullTerminatedString(encoding);
                InsertInTree(new GaussDBTsQueryLexeme(str, weight, prefix), nodes, ref value);
                continue;

            case 2: // operation
                if (reader.ShouldBuffer(sizeof(byte)))
                    await reader.Buffer(async, sizeof(byte), cancellationToken).ConfigureAwait(false);
                var kind = (GaussDBTsQuery.NodeKind)reader.ReadByte();

                GaussDBTsQuery node;
                switch (kind)
                {
                case Not:
                    node = new GaussDBTsQueryNot(null!);
                    InsertInTree(node, nodes, ref value);
                    nodes.Push((node, 0));
                    continue;

                case And:
                    node = new GaussDBTsQueryAnd(null!, null!);
                    break;
                case Or:
                    node = new GaussDBTsQueryOr(null!, null!);
                    break;
                case Phrase:
                    if (reader.ShouldBuffer(sizeof(short)))
                        await reader.Buffer(async, sizeof(short), cancellationToken).ConfigureAwait(false);
                    node = new GaussDBTsQueryFollowedBy(null!, reader.ReadInt16(), null!);
                    break;
                default:
                    throw new UnreachableException(
                        $"Internal GaussDB bug: unexpected value {kind} of enum {nameof(GaussDBTsQuery.NodeKind)}. Please file a bug.");
                }

                InsertInTree(node, nodes, ref value);

                nodes.Push((node, 1));
                nodes.Push((node, 2));
                continue;

            case var tokenType:
                throw new UnreachableException(
                    $"Internal GaussDB bug: unexpected token type {tokenType} when reading tsquery. Please file a bug.");
            }
        }

        if (nodes.Count != 0)
            throw new UnreachableException("Internal GaussDB bug, please report.");

        return value!;

        static void InsertInTree(GaussDBTsQuery node, Stack<(GaussDBTsQuery Node, int Location)> nodes, ref GaussDBTsQuery? value)
        {
            if (nodes.Count == 0)
                value = node;
            else
            {
                var parent = nodes.Pop();
                switch (parent.Location)
                {
                case 0:
                    ((GaussDBTsQueryNot)parent.Node).Child = node;
                    break;
                case 1:
                    ((GaussDBTsQueryBinOp)parent.Node).Left = node;
                    break;
                case 2:
                    ((GaussDBTsQueryBinOp)parent.Node).Right = node;
                    break;
                default:
                    throw new UnreachableException("Internal GaussDB bug, please report.");
                }
            }
        }
    }

    public override Size GetSize(SizeContext context, T value, ref object? writeState)
        => value.Kind is Empty
            ? 4
            : 4 + GetNodeLength(value);

    int GetNodeLength(GaussDBTsQuery node)
        => node.Kind switch
        {
            Lexeme when encoding.GetByteCount(((GaussDBTsQueryLexeme)node).Text) is var strLen
                => strLen > 2046
                    ? throw new InvalidCastException("Lexeme text too long. Must be at most 2046 encoded bytes.")
                    : 4 + strLen,
            And or Or => 2 + GetNodeLength(((GaussDBTsQueryBinOp)node).Left) + GetNodeLength(((GaussDBTsQueryBinOp)node).Right),
            Not => 2 + GetNodeLength(((GaussDBTsQueryNot)node).Child),
            Empty => throw new InvalidOperationException("Empty tsquery nodes must be top-level"),

            // 2 additional bytes for uint16 phrase operator "distance" field.
            Phrase => 4 + GetNodeLength(((GaussDBTsQueryBinOp)node).Left) + GetNodeLength(((GaussDBTsQueryBinOp)node).Right),

            _ => throw new UnreachableException(
                $"Internal GaussDB bug: unexpected value {node.Kind} of enum {nameof(GaussDBTsQuery.NodeKind)}. Please file a bug.")
        };

    public override void Write(PgWriter writer, T value)
        => Write(async: false, writer, value, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, T value, CancellationToken cancellationToken = default)
        => Write(async: true, writer, value, cancellationToken);

    async ValueTask Write(bool async, PgWriter writer, GaussDBTsQuery value, CancellationToken cancellationToken)
    {
        var numTokens = GetTokenCount(value);

        if (writer.ShouldFlush(sizeof(int)))
            await writer.Flush(async, cancellationToken).ConfigureAwait(false);
        writer.WriteInt32(numTokens);

        if (numTokens is 0)
            return;

        await WriteCore(value).ConfigureAwait(false);

        async Task WriteCore(GaussDBTsQuery node)
        {
            if (writer.ShouldFlush(sizeof(byte)))
                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            writer.WriteByte(node.Kind is Lexeme ? (byte)1 : (byte)2);

            if (node.Kind is Lexeme)
            {
                var lexemeNode = (GaussDBTsQueryLexeme)node;

                if (writer.ShouldFlush(sizeof(byte) + sizeof(byte)))
                    await writer.Flush(async, cancellationToken).ConfigureAwait(false);

                writer.WriteByte((byte)lexemeNode.Weights);
                writer.WriteByte(lexemeNode.IsPrefixSearch ? (byte)1 : (byte)0);

                if (async)
                    await writer.WriteCharsAsync(lexemeNode.Text.AsMemory(), encoding, cancellationToken).ConfigureAwait(false);
                else
                    writer.WriteChars(lexemeNode.Text.AsMemory().Span, encoding);

                if (writer.ShouldFlush(sizeof(byte)))
                    await writer.Flush(async, cancellationToken).ConfigureAwait(false);

                writer.WriteByte(0);
                return;
            }

            writer.WriteByte((byte)node.Kind);

            switch (node.Kind)
            {
            case Not:
                await WriteCore(((GaussDBTsQueryNot)node).Child).ConfigureAwait(false);
                return;
            case Phrase:
                writer.WriteInt16(((GaussDBTsQueryFollowedBy)node).Distance);
                break;
            }

            await WriteCore(((GaussDBTsQueryBinOp)node).Right).ConfigureAwait(false);
            await WriteCore(((GaussDBTsQueryBinOp)node).Left).ConfigureAwait(false);
        }
    }

    int GetTokenCount(GaussDBTsQuery node)
        => node.Kind switch
        {
            Lexeme => 1,
            And or Or or Phrase => 1 + GetTokenCount(((GaussDBTsQueryBinOp)node).Left) + GetTokenCount(((GaussDBTsQueryBinOp)node).Right),
            Not => 1 + GetTokenCount(((GaussDBTsQueryNot)node).Child),
            Empty => 0,

            _ => throw new UnreachableException(
                $"Internal GaussDB bug: unexpected value {node.Kind} of enum {nameof(GaussDBTsQuery.NodeKind)}. Please file a bug.")
        };
}
