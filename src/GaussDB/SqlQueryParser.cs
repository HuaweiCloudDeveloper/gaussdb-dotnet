using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace HuaweiCloud.GaussDB;

sealed class SqlQueryParser
{
    static GaussDBParameterCollection EmptyParameters { get; } = [];

    readonly Dictionary<string, int> _paramIndexMap = new(StringComparer.OrdinalIgnoreCase);
    readonly StringBuilder _rewrittenSql = new();

    /// <summary>
    /// <p>
    /// Receives a user SQL query as passed in by the user in <see cref="GaussDBCommand.CommandText"/> or
    /// <see cref="GaussDBBatchCommand.CommandText"/>, and rewrites it for PostgreSQL compatibility.
    /// </p>
    /// <p>
    /// This includes doing rewriting named parameter placeholders to positional (@p => $1), and splitting the query
    /// up by semicolons (legacy batching, SELECT 1; SELECT 2).
    /// </p>
    /// </summary>
    /// <param name="command">The user-facing <see cref="GaussDBCommand"/> being executed.</param>
    /// <param name="standardConformingStrings">Whether PostgreSQL standards-conforming are used.</param>
    /// <param name="deriveParameters">
    /// A bool indicating whether parameters contains a list of preconfigured parameters or an empty list to be filled with derived
    /// parameters.
    /// </param>
    internal void ParseRawQuery(
        GaussDBCommand? command,
        bool standardConformingStrings = true,
        bool deriveParameters = false)
        => ParseRawQuery(command, batchCommand: null, standardConformingStrings, deriveParameters);

    /// <summary>
    /// <p>
    /// Receives a user SQL query as passed in by the user in <see cref="GaussDBCommand.CommandText"/> or
    /// <see cref="GaussDBBatchCommand.CommandText"/>, and rewrites it for PostgreSQL compatibility.
    /// </p>
    /// <p>
    /// This includes doing rewriting named parameter placeholders to positional (@p => $1), and splitting the query
    /// up by semicolons (legacy batching, SELECT 1; SELECT 2).
    /// </p>
    /// </summary>
    /// <param name="batchCommand"> The user-facing <see cref="GaussDBBatchCommand"/> being executed.</param>
    /// <param name="standardConformingStrings">Whether PostgreSQL standards-conforming are used.</param>
    /// <param name="deriveParameters">
    /// A bool indicating whether parameters contains a list of preconfigured parameters or an empty list to be filled with derived
    /// parameters.
    /// </param>
    internal void ParseRawQuery(
        GaussDBBatchCommand? batchCommand,
        bool standardConformingStrings = true,
        bool deriveParameters = false)
        => ParseRawQuery(command: null, batchCommand, standardConformingStrings, deriveParameters);

    void ParseRawQuery(
        GaussDBCommand? command,
        GaussDBBatchCommand? batchCommand,
        bool standardConformingStrings = true,
        bool deriveParameters = false)
    {
        string sql;
        GaussDBParameterCollection parameters;
        List<GaussDBBatchCommand>? batchCommands;

        var statementIndex = 0;
        if (command is null)
        {
            // Batching mode. We're processing only one batch - if we encounter a semicolon (legacy batching), that's an error.
            Debug.Assert(batchCommand is not null);
            sql = batchCommand.CommandText;
            parameters = batchCommand._parameters ?? EmptyParameters;
            batchCommands = null;
        }
        else
        {
            // Command mode. Semicolons (legacy batching) may occur.
            Debug.Assert(batchCommand is null);
            sql = command.CommandText;
            parameters = command._parameters ?? EmptyParameters;
            batchCommands = command.InternalBatchCommands;
            MoveToNextBatchCommand();
        }

        Debug.Assert(batchCommand is not null);
        Debug.Assert(parameters.PlaceholderType != PlaceholderType.Positional);
        Debug.Assert(deriveParameters == false || parameters.Count == 0);
        // Debug.Assert(batchCommand.PositionalParameters is not null && batchCommand.PositionalParameters.Count == 0);

        _paramIndexMap.Clear();
        _rewrittenSql.Clear();

        var currCharOfs = 0;
        var end = sql.Length;
        var ch = '\0';
        int dollarTagStart;
        int dollarTagEnd;
        var currTokenBeg = 0;
        var blockCommentLevel = 0;
        var parenthesisLevel = 0;

        None:
        if (currCharOfs >= end)
            goto Finish;
        var lastChar = ch;
        ch = sql[currCharOfs++];
        NoneContinue:
        while (true)
        {
            switch (ch)
            {
            case '/':
                goto BlockCommentBegin;
            case '-':
                goto LineCommentBegin;
            case '\'':
                if (standardConformingStrings)
                    goto Quoted;
                goto Escaped;
            case '$':
                if (!IsIdentifier(lastChar))
                    goto DollarQuotedStart;
                break;
            case '"':
                goto Quoted;
            case ':':
                if (lastChar != ':')
                    goto NamedParamStart;
                break;
            case '@':
                if (lastChar != '@')
                    goto NamedParamStart;
                break;
            case ';':
                if (parenthesisLevel == 0)
                    goto SemiColon;
                break;
            case '(':
                parenthesisLevel++;
                break;
            case ')':
                parenthesisLevel--;
                break;
            case 'e':
            case 'E':
                if (!IsLetter(lastChar))
                    goto EscapedStart;
                break;
            }

            if (currCharOfs >= end)
                goto Finish;

            lastChar = ch;
            ch = sql[currCharOfs++];
        }

        NamedParamStart:
        if (currCharOfs < end)
        {
            lastChar = ch;
            ch = sql[currCharOfs];
            if (IsParamNameChar(ch))
            {
                if (currCharOfs - 1 > currTokenBeg)
                    _rewrittenSql.Append(sql, currTokenBeg, currCharOfs - 1 - currTokenBeg);
                currTokenBeg = currCharOfs++ - 1;
                goto NamedParam;
            }
            currCharOfs++;
            goto NoneContinue;
        }
        goto Finish;

        NamedParam:
        // We have already at least one character of the param name
        while (true)
        {
            lastChar = ch;
            if (currCharOfs >= end || !IsParamNameChar(ch = sql[currCharOfs]))
            {
                var paramName = sql.Substring(currTokenBeg + 1, currCharOfs - (currTokenBeg + 1));

                if (!_paramIndexMap.TryGetValue(paramName, out var index))
                {
                    // Parameter hasn't been seen before in this query
                    if (!parameters.TryGetValue(paramName, out var parameter))
                    {
                        if (deriveParameters)
                        {
                            parameter = new GaussDBParameter { ParameterName = paramName };
                            parameters.Add(parameter);
                        }
                        else
                        {
                            // Parameter placeholder does not match a parameter on this command.
                            // Leave the text as it was in the SQL, it may not be a an actual placeholder
                            _rewrittenSql.Append(sql, currTokenBeg, currCharOfs - currTokenBeg);
                            currTokenBeg = currCharOfs;
                            if (currCharOfs >= end)
                                goto Finish;

                            currCharOfs++;
                            goto NoneContinue;
                        }
                    }

                    if (!parameter.IsInputDirection)
                        ThrowHelper.ThrowInvalidOperationException("Parameter '{0}' referenced in SQL but is an out-only parameter", paramName);

                    batchCommand.PositionalParameters.Add(parameter);
                    index = _paramIndexMap[paramName] = batchCommand.PositionalParameters.Count;
                }
                _rewrittenSql.Append('$');
                _rewrittenSql.Append(index);
                currTokenBeg = currCharOfs;

                if (currCharOfs >= end)
                    goto Finish;

                currCharOfs++;
                goto NoneContinue;
            }

            currCharOfs++;
        }

        Quoted:
        Debug.Assert(ch == '\'' || ch == '"');
        while (currCharOfs < end && sql[currCharOfs] != ch)
        {
            currCharOfs++;
        }
        if (currCharOfs < end)
        {
            currCharOfs++;
            ch = '\0';
            goto None;
        }
        goto Finish;

        EscapedStart:
        if (currCharOfs < end)
        {
            lastChar = ch;
            ch = sql[currCharOfs++];
            if (ch == '\'')
                goto Escaped;
            goto NoneContinue;
        }
        goto Finish;

        Escaped:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            switch (ch)
            {
            case '\'':
                goto MaybeConcatenatedEscaped;
            case '\\':
            {
                if (currCharOfs >= end)
                    goto Finish;
                currCharOfs++;
                break;
            }
            }
        }
        goto Finish;

        MaybeConcatenatedEscaped:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            switch (ch)
            {
            case '\r':
            case '\n':
                goto MaybeConcatenatedEscaped2;
            case ' ':
            case '\t':
            case '\f':
                continue;
            default:
                lastChar = '\0';
                goto NoneContinue;
            }
        }
        goto Finish;

        MaybeConcatenatedEscaped2:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            switch (ch)
            {
            case '\'':
                goto Escaped;
            case '-':
            {
                if (currCharOfs >= end)
                    goto Finish;
                ch = sql[currCharOfs++];
                if (ch == '-')
                    goto MaybeConcatenatedEscapeAfterComment;
                lastChar = '\0';
                goto NoneContinue;
            }
            case ' ':
            case '\t':
            case '\n':
            case '\r':
            case '\f':
                continue;
            default:
                lastChar = '\0';
                goto NoneContinue;
            }
        }
        goto Finish;

        MaybeConcatenatedEscapeAfterComment:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            if (ch == '\r' || ch == '\n')
                goto MaybeConcatenatedEscaped2;
        }
        goto Finish;

        DollarQuotedStart:
        if (currCharOfs < end)
        {
            ch = sql[currCharOfs];
            if (ch == '$')
            {
                // Empty tag
                dollarTagStart = dollarTagEnd = currCharOfs;
                currCharOfs++;
                goto DollarQuoted;
            }
            if (IsIdentifierStart(ch))
            {
                dollarTagStart = currCharOfs;
                currCharOfs++;
                goto DollarQuotedInFirstDelim;
            }
            lastChar = '$';
            currCharOfs++;
            goto NoneContinue;
        }
        goto Finish;

        DollarQuotedInFirstDelim:
        while (currCharOfs < end)
        {
            lastChar = ch;
            ch = sql[currCharOfs++];
            if (ch == '$')
            {
                dollarTagEnd = currCharOfs - 1;
                goto DollarQuoted;
            }
            if (!IsDollarTagIdentifier(ch))
                goto NoneContinue;
        }
        goto Finish;

        DollarQuoted:
        var tag = sql.AsSpan(dollarTagStart - 1, dollarTagEnd - dollarTagStart + 2);
        var pos = sql.AsSpan(dollarTagEnd + 1).IndexOf(tag);
        if (pos == -1)
        {
            currCharOfs = end;
            goto Finish;
        }
        pos += dollarTagEnd + 1; // If the substring is found adjust the position to be relative to the entire string
        currCharOfs = pos + dollarTagEnd - dollarTagStart + 2;
        ch = '\0';
        goto None;

        LineCommentBegin:
        if (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            if (ch == '-')
                goto LineComment;
            lastChar = '\0';
            goto NoneContinue;
        }
        goto Finish;

        LineComment:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            if (ch == '\r' || ch == '\n')
                goto None;
        }
        goto Finish;

        BlockCommentBegin:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            if (ch == '*')
            {
                blockCommentLevel++;
                goto BlockComment;
            }
            if (ch != '/')
            {
                if (blockCommentLevel > 0)
                    goto BlockComment;
                lastChar = '\0';
                goto NoneContinue;
            }
        }
        goto Finish;

        BlockComment:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            switch (ch)
            {
            case '*':
                goto BlockCommentEnd;
            case '/':
                goto BlockCommentBegin;
            }
        }
        goto Finish;

        BlockCommentEnd:
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs++];
            if (ch == '/')
            {
                if (--blockCommentLevel > 0)
                    goto BlockComment;
                goto None;
            }
            if (ch != '*')
                goto BlockComment;
        }
        goto Finish;

        SemiColon:
        _rewrittenSql.Append(sql, currTokenBeg, currCharOfs - currTokenBeg - 1);
        batchCommand.FinalCommandText = _rewrittenSql.ToString();
        while (currCharOfs < end)
        {
            ch = sql[currCharOfs];
            if (char.IsWhiteSpace(ch))
            {
                currCharOfs++;
                continue;
            }
            // TODO: Handle end of line comment? Although psql doesn't seem to handle them...

            // We've found a non-whitespace character after a semicolon - this is legacy batching.

            if (command is null)
            {
                ThrowHelper.ThrowNotSupportedException($"Specifying multiple SQL statements in a single {nameof(GaussDBBatchCommand)} isn't supported, " +
                                                       "please remove all semicolons.");
            }

            statementIndex++;
            MoveToNextBatchCommand();
            _paramIndexMap.Clear();
            _rewrittenSql.Clear();

            currTokenBeg = currCharOfs;
            goto None;
        }
        if (batchCommands is not null && batchCommands.Count > statementIndex + 1)
            batchCommands.RemoveRange(statementIndex + 1, batchCommands.Count - (statementIndex + 1));
        return;

        Finish:
        _rewrittenSql.Append(sql, currTokenBeg, end - currTokenBeg);
        if (statementIndex is 0 && _paramIndexMap.Count is 0)
            // Single statement, no parameters, no rewriting necessary
            batchCommand.FinalCommandText = sql;
        else
            batchCommand.FinalCommandText = _rewrittenSql.ToString();
        if (batchCommands is not null && batchCommands.Count > statementIndex + 1)
            batchCommands.RemoveRange(statementIndex + 1, batchCommands.Count - (statementIndex + 1));

        void MoveToNextBatchCommand()
        {
            Debug.Assert(batchCommands is not null);
            if (batchCommands.Count > statementIndex)
            {
                batchCommand = batchCommands[statementIndex];
                batchCommand.Reset();
                batchCommand._parameters = parameters;
            }
            else
            {
                batchCommand = new GaussDBBatchCommand { _parameters = parameters };
                batchCommands.Add(batchCommand);
            }
        }
    }

    // Is ASCII letter comparison optimization https://github.com/dotnet/runtime/blob/60cfaec2e6cffeb9a006bec4b8908ffcf71ac5b4/src/libraries/System.Private.CoreLib/src/System/Char.cs#L236

    static bool IsLetter(char ch)
        // [a-zA-Z]
        => (uint)((ch | 0x20) - 'a') <= ('z' - 'a');

    static bool IsIdentifierStart(char ch)
        // [a-zA-Z_\x80-\xFF]
        => (uint)((ch | 0x20) - 'a') <= ('z' - 'a') || ch == '_' || (uint)(ch - 128) <= 127u;

    static bool IsDollarTagIdentifier(char ch)
        // [a-zA-Z0-9_\x80-\xFF]
        => (uint)((ch | 0x20) - 'a') <= ('z' - 'a') || (uint)(ch - '0') <= ('9' - '0') || ch == '_' || (uint)(ch - 128) <= 127u;

    static bool IsIdentifier(char ch)
        // [a-zA-Z0-9_$\x80-\xFF]
        => (uint)((ch | 0x20) - 'a') <= ('z' - 'a') || (uint)(ch - '0') <= ('9' - '0') || ch == '_' || ch == '$' || (uint)(ch - 128) <= 127u;

    static bool IsParamNameChar(char ch)
        => char.IsLetterOrDigit(ch) || ch == '_' || ch == '.';  // why dot??
}
