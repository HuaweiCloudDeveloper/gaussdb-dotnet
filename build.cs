var hasExplicitTarget = Array.Exists(args, arg => arg == "--target" || arg.StartsWith("--target=", StringComparison.Ordinal));
var effectiveArgs = args;
if (!hasExplicitTarget)
{
    effectiveArgs = new string[args.Length + 1];
    effectiveArgs[0] = "--target=publish";
    Array.Copy(args, 0, effectiveArgs, 1, args.Length);
}

var target = CommandLineParser.Val(effectiveArgs, "target", "publish");
var apiKey = CommandLineParser.Val(effectiveArgs, "apiKey");
var noPush = CommandLineParser.BooleanVal(effectiveArgs, "noPush");
var version = Environment.GetEnvironmentVariable("VERSION");
var stable = CommandLineParser.BooleanVal(effectiveArgs, "stable") || !string.IsNullOrEmpty(version);
var runningOnGithubActions = Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
var configuredTestProfile = Environment.GetEnvironmentVariable("GAUSSDB_TEST_PROFILE");
var testProfile = string.IsNullOrWhiteSpace(configuredTestProfile)
    ? (runningOnGithubActions ? "ci-baseline" : "")
    : configuredTestProfile;
var runningBaselineCi = string.Equals(testProfile, "ci-baseline", StringComparison.OrdinalIgnoreCase);
var runningLocalProductTest = string.Equals(testProfile, "local-product", StringComparison.OrdinalIgnoreCase);

// Keep these exclusions narrow. CI baseline exclusions are documented in .github/workflows/test.yml.
const string BaselineCiTestFilter =
    "FullyQualifiedName!~HuaweiCloud.GaussDB.Tests.SecurityTests&" +
    "FullyQualifiedName!~HuaweiCloud.GaussDB.Tests.Replication&" +
    "FullyQualifiedName!~Open_physical_failure&" +
    "FullyQualifiedName!~BaseColumnName_with_column_aliases";
// Local product runs target a remote single-node GaussDB environment, so they keep CI-valid localhost
// topology tests out of the local profile without weakening the CI baseline.
// - Open_physical_failure: current fork expects TimeoutException while upstream expects SocketException.
// - BaseColumnName_with_column_aliases: fixed temp table name can survive pooled sessions and fail with 42P07.
// - IntegrationTest: hardcodes localhost,127.0.0.1 and requires a local multi-host topology.
// - Multiple_hosts_with_disabled_sql_rewriting: also hardcodes localhost,127.0.0.1.
const string LocalProductTestFilter =
    "FullyQualifiedName!~Open_physical_failure&" +
    "FullyQualifiedName!~BaseColumnName_with_column_aliases&" +
    "FullyQualifiedName!~HuaweiCloud.GaussDB.Tests.MultipleHostsTests.IntegrationTest&" +
    "FullyQualifiedName!~Multiple_hosts_with_disabled_sql_rewriting";
var testFilter = NormalizeTestFilter(Environment.GetEnvironmentVariable("GAUSSDB_TEST_FILTER"));
if (string.IsNullOrEmpty(testFilter))
{
    if (runningBaselineCi)
        testFilter = BaselineCiTestFilter;
    else if (runningLocalProductTest)
        testFilter = LocalProductTestFilter;
}

Console.WriteLine($$"""
Arguments:

target: {{target}}
stable: {{stable}}
noPush: {{noPush}}
testProfile: {{(string.IsNullOrEmpty(testProfile) ? "(none)" : testProfile)}}
testFilter: {{(string.IsNullOrEmpty(testFilter) ? "(none)" : testFilter)}}
args:
{{effectiveArgs.StringJoin("\n")}}

""");

var solutionPath = "./GaussDB.slnx";
string[] srcProjects = [
    "./src/GaussDB/GaussDB.csproj"
];
string[] testProjects = [
    "./test/GaussDB.Tests/GaussDB.Tests.csproj",
    "./test/GaussDB.DependencyInjection.Tests/GaussDB.DependencyInjection.Tests.csproj"
];

var process = DotNetPackageBuildProcess.Create(options =>
{
    options.SolutionPath = solutionPath;
    options.SrcProjects = srcProjects;
    options.TestProjects = testProjects;
    options.ArtifactsPath = "./artifacts/packages";

    options.WithTaskConfigure("build", task => task
        .WithDescription("build")
        .WithExecution(cancellationToken => ExecuteCommandAsync($"dotnet build {solutionPath}", cancellationToken)));

    options.WithTaskConfigure("test", task => task
        .WithDescription("dotnet test")
        .WithDependency("build")
        .WithExecution(async cancellationToken =>
        {
            foreach (var project in testProjects)
            {
                var loggerOptions = runningOnGithubActions
                    ? "--logger GitHubActions"
                    : "--logger \"console;verbosity=d\"";
                var filterOptions = string.Empty;
                if (!string.IsNullOrEmpty(testFilter) &&
                    project.EndsWith("GaussDB.Tests.csproj", StringComparison.Ordinal))
                {
                    filterOptions = $" --filter \"{testFilter}\"";
                }

                var command =
                    $"dotnet test --blame --collect:\"XPlat Code Coverage;Format=cobertura,opencover;ExcludeByAttribute=ExcludeFromCodeCoverage,Obsolete,GeneratedCode,CompilerGenerated\" {loggerOptions}{filterOptions} -v=d {project}";
                await ExecuteCommandAsync(command, cancellationToken);
            }
        }));

    options.WithTaskConfigure("publish", task => task
        .WithDescription("dotnet pack")
        .WithDependency("build")
        .WithExecution(PackAndMaybePushAsync));
});

Console.WriteLine("Cleaning previous package artifacts if they exist.");
if (Directory.Exists("./artifacts/packages"))
    Directory.Delete("./artifacts/packages", true);

await process.ExecuteAsync(effectiveArgs, ApplicationHelper.ExitToken);

static string? NormalizeTestFilter(string? filter)
{
    if (string.IsNullOrWhiteSpace(filter))
        return null;

    var parts = filter.Split(
        new[] { "\r\n", "\n", "\r" },
        StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    return string.Join("&", parts);
}

async Task PackAndMaybePushAsync(CancellationToken cancellationToken)
{
    // The script owns package cleanup and publishing so local and CI runs stay aligned.
    if (Directory.Exists("./artifacts/packages"))
        Directory.Delete("./artifacts/packages", true);

    var packOptions = " -o ./artifacts/packages";
    if (stable)
    {
        if (!string.IsNullOrEmpty(version))
            packOptions += $" -p VersionPrefix={version}";
    }
    else
    {
        var suffix = $"preview-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
        packOptions += $" --version-suffix {suffix}";
    }

    foreach (var project in srcProjects)
        await ExecuteCommandAsync($"dotnet pack {project} {packOptions}", cancellationToken);

    if (noPush)
    {
        Console.WriteLine("Skip push there's noPush specified");
        return;
    }

    if (string.IsNullOrEmpty(apiKey))
    {
        apiKey = Environment.GetEnvironmentVariable("NUGET_API_KEY");

        if (string.IsNullOrEmpty(apiKey))
        {
            Console.WriteLine("Skip push since there's no apiKey found");
            return;
        }
    }

    foreach (var file in Directory.GetFiles("./artifacts/packages/", "*.nupkg"))
    {
        await RetryHelper.TryInvokeAsync(
            () => ExecuteCommandAsync($"dotnet nuget push {file} -s https://api.nuget.org/v3/index.json -k {apiKey} --skip-duplicate", cancellationToken),
            cancellationToken: cancellationToken);
    }
}

async Task ExecuteCommandAsync(string commandText, CancellationToken cancellationToken = default)
{
    Console.WriteLine($"Executing command: \n    {commandText}");
    Console.WriteLine();

    var result = await CommandExecutor.ExecuteCommandAndOutputAsync(commandText, cancellationToken: cancellationToken);
    result.EnsureSuccessExitCode();
    Console.WriteLine();
}
