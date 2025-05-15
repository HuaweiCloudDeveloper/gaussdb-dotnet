using GaussDB.BackendMessages;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using GaussDB.Internal;

namespace GaussDB;

static class ThrowHelper
{
    [DoesNotReturn]
    internal static void ThrowArgumentOutOfRangeException()
        => throw new ArgumentOutOfRangeException();

    [DoesNotReturn]
    internal static void ThrowArgumentOutOfRangeException(string paramName, string message)
        => throw new ArgumentOutOfRangeException(paramName, message);

    [DoesNotReturn]
    internal static void ThrowArgumentOutOfRangeException(string paramName, string message, object argument)
        => throw new ArgumentOutOfRangeException(paramName, string.Format(message, argument));

    [DoesNotReturn]
    internal static void ThrowUnreachableException(string message, object argument)
        => throw new UnreachableException(string.Format(message, argument));

    [DoesNotReturn]
    internal static void ThrowInvalidOperationException()
        => throw new InvalidOperationException();

    [DoesNotReturn]
    internal static void ThrowInvalidOperationException(string message)
        => throw new InvalidOperationException(message);

    [DoesNotReturn]
    internal static void ThrowInvalidOperationException(string message, object argument)
        => throw new InvalidOperationException(string.Format(message, argument));

    [DoesNotReturn]
    internal static void ThrowObjectDisposedException(string? objectName)
        => throw new ObjectDisposedException(objectName);

    [DoesNotReturn]
    internal static void ThrowObjectDisposedException(string objectName, string message)
        => throw new ObjectDisposedException(objectName, message);

    [DoesNotReturn]
    internal static void ThrowObjectDisposedException(string objectName, Exception? innerException)
        => throw new ObjectDisposedException(objectName, innerException);

    [DoesNotReturn]
    internal static void ThrowInvalidCastException(string message, object argument)
        => throw new InvalidCastException(string.Format(message, argument));

    [DoesNotReturn]
    internal static void ThrowInvalidCastException_NoValue(FieldDescription field) =>
        throw new InvalidCastException($"Column '{field.Name}' is null.");

    [DoesNotReturn]
    internal static void ThrowInvalidCastException(string message) =>
        throw new InvalidCastException(message);

    [DoesNotReturn]
    internal static void ThrowInvalidCastException_NoValue() =>
        throw new InvalidCastException("Field is null.");

    [DoesNotReturn]
    internal static void ThrowGaussDBException(string message)
        => throw new GaussDBException(message);

    [DoesNotReturn]
    internal static void ThrowGaussDBException(string message, Exception? innerException)
        => throw new GaussDBException(message, innerException);

    [DoesNotReturn]
    internal static void ThrowGaussDBOperationInProgressException(GaussDBCommand command)
        => throw new GaussDBOperationInProgressException(command);

    [DoesNotReturn]
    internal static void ThrowGaussDBOperationInProgressException(ConnectorState state)
        => throw new GaussDBOperationInProgressException(state);

    [DoesNotReturn]
    internal static void ThrowArgumentException(string message)
        => throw new ArgumentException(message);

    [DoesNotReturn]
    internal static void ThrowArgumentException(string message, string paramName)
        => throw new ArgumentException(message, paramName);

    [DoesNotReturn]
    internal static void ThrowArgumentNullException(string message, string paramName)
        => throw new ArgumentNullException(paramName, message);

    [DoesNotReturn]
    internal static void ThrowIndexOutOfRangeException(string message)
        => throw new IndexOutOfRangeException(message);

    [DoesNotReturn]
    internal static void ThrowIndexOutOfRangeException(string message, object argument)
        => throw new IndexOutOfRangeException(string.Format(message, argument));

    [DoesNotReturn]
    internal static void ThrowNotSupportedException(string? message = null)
        => throw new NotSupportedException(message);

    [DoesNotReturn]
    internal static void ThrowGaussDBExceptionWithInnerTimeoutException(string message)
        => throw new GaussDBException(message, new TimeoutException());
}
