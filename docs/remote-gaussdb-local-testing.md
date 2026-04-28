# Remote GaussDB Local Testing

This document describes how to run the GaussDB .NET driver test suite against a remote
single-node GaussDB server from a local development machine.

The GitHub Actions baseline uses openGauss and cannot provision the closed-source GaussDB
environment. For GaussDB product compatibility work, local remote-server validation is the
primary test path.

## Prerequisites

- A reachable GaussDB server.
- A test login user that can create and drop a disposable database.
- .NET 8 SDK/runtime on the client machine.
- `gsql` is recommended for creating and dropping the disposable test database.

Do not run the test suite against a shared or important database. The tests create, modify,
and drop many objects.

## Test Profile

Use the `local-product` test profile for remote single-node GaussDB runs:

```powershell
$env:GAUSSDB_TEST_PROFILE = "local-product"
```

This profile keeps the local run focused on cases valid for the current remote single-node
topology. It filters a small number of known outliers in `build.cs`, including loopback
multi-host tests that require `localhost,127.0.0.1` on the database host.

Do not set `GAUSSDB_TEST_FILTER` unless intentionally overriding the profile filter.

## Create A Disposable Test Database

Create a new database for each run:

```powershell
$hostName = "<gaussdb-host>"
$port = "<gaussdb-port>"
$adminDb = "<existing-admin-db>"
$user = "<test-user>"
$password = "<test-password>"
$testDb = "gaussdb_driver_test_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
$gsql = "<path-to-gsql>"

& $gsql `
  -d $adminDb `
  -h $hostName `
  -U $user `
  -p $port `
  -W $password `
  -c "CREATE DATABASE $testDb;"
```

Then point the driver tests at the disposable database:

```powershell
$env:NPGSQL_TEST_DB = "Host=$hostName;Port=$port;Database=$testDb;Username=$user;Password=$password"
```

## Run The Tests

Make sure local CI flags are not set. These variables make some environment checks fail
instead of being ignored:

```powershell
Remove-Item Env:\CI -ErrorAction SilentlyContinue
Remove-Item Env:\GITHUB_ACTIONS -ErrorAction SilentlyContinue
Remove-Item Env:\GAUSSDB_TEST_FILTER -ErrorAction SilentlyContinue
```

Restore the local tool manifest and run the test target:

```powershell
dotnet tool restore
dotnet tool run dotnet-exec ./build.cs --args --target=test
```

If multiple dotnet installations are present, verify that the selected one can run `net8.0`:

```powershell
dotnet --info
```

On Windows, if the system-wide dotnet install does not contain the .NET 8 runtime, prepend
the user-local dotnet install before running tests:

```powershell
$env:DOTNET_ROOT = Join-Path $env:USERPROFILE ".dotnet"
$env:PATH = $env:DOTNET_ROOT + [IO.Path]::PathSeparator + $env:PATH
$env:DOTNET_MULTILEVEL_LOOKUP = "0"
```

## Clean Up

Drop the disposable database after the run:

```powershell
& $gsql `
  -d $adminDb `
  -h $hostName `
  -U $user `
  -p $port `
  -W $password `
  -c "DROP DATABASE IF EXISTS $testDb;"
```

## Replication Tests

Replication tests do not need a separate local profile switch. They check server settings
such as `wal_level` and `max_wal_senders` in their setup.

If the remote GaussDB server is not provisioned for logical replication, those tests are
ignored during local runs. On build servers, the same missing prerequisites are treated as
failures.

## Expected Baseline

The latest verified local product-profile run used a disposable remote GaussDB database and
completed successfully:

- `GaussDB.Tests`: 3090 total / 2592 passed / 498 skipped / 0 failed
- `GaussDB.DependencyInjection.Tests`: 26 total / 26 passed / 0 failed

Skipped tests include cases ignored by the existing test suite due to unavailable optional
server prerequisites, such as logical replication.

