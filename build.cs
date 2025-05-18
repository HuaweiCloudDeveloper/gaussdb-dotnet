var target = CommandLineParser.Val(args, "target", "Default");
var apiKey = CommandLineParser.Val(args, "apiKey");
var noPush = CommandLineParser.BooleanVal(args, "noPush");
var version = CommandLineParser.Val(args, "VERSION");
var stable = CommandLineParser.BooleanVal(args, "stable") || !string.IsNullOrEmpty(version);
var runningOnGithubActions = Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";

Console.WriteLine($$"""
Arguments:

target: {{target}}
stable: {{stable}}
noPush: {{noPush}}
args:
{{args.StringJoin("\n")}}

""");

var solutionPath = "./GaussDB.slnx";
string[] srcProjects = [
    "./src/GaussDB/GaussDB.csproj"
];
string[] testProjects = [
    "./test/GaussDB.Tests/GaussDB.Tests.csproj",
    "./test/GaussDB.GaussDB.DependencyInjection.Tests/GaussDB.GaussDB.DependencyInjection.Tests.csproj"
];

await new BuildProcessBuilder()
    .WithSetup(() =>
    {
        // cleanup previous artifacts
        if (Directory.Exists("./artifacts/packages"))
            Directory.Delete("./artifacts/packages", true);
    })
    .WithTaskExecuting(task => Console.WriteLine($@"===== Task [{task.Name}] {task.Description} executing ======"))
    .WithTaskExecuted(task => Console.WriteLine($@"===== Task [{task.Name}] {task.Description} executed ======"))
    .WithTask("build", b =>
    {
        b.WithDescription("build")
            .WithExecution(cancellationToken => ExecuteCommandAsync($"dotnet build {solutionPath}", cancellationToken))
            ;
    })
    .WithTask("test", b =>
    {
        b.WithDescription("dotnet test")
            .WithDependency("build")
            .WithExecution(async cancellationToken =>
            {
                foreach (var project in testProjects)
                {
                    var loggerOptions = runningOnGithubActions
                        ? "--logger GitHubActions"
                        : "--logger \"console;verbosity=d\"";
                    var command = $"dotnet test --blame --collect:\"XPlat Code Coverage;Format=cobertura,opencover;ExcludeByAttribute=ExcludeFromCodeCoverage,Obsolete,GeneratedCode,CompilerGenerated\" {loggerOptions} -v=d {project}";
                    await ExecuteCommandAsync(command, cancellationToken);
                }
            })
            ;
    })
    .WithTask("pack", b => b
        .WithDescription("dotnet pack")
        .WithDependency("build")
        .WithExecution(async cancellationToken =>
        {
            var packOptions = " -o ./artifacts/packages";
            if (stable)
            {
                if (!string.IsNullOrEmpty(version))
                {
                    packOptions = $" -o ./artifacts/packages --version-prefix {version}";
                }
            }
            else
            {
                var suffix = $"preview-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
                packOptions += $" --version-suffix {suffix}";
            }

            foreach (var project in srcProjects)
            {
                await ExecuteCommandAsync($"dotnet pack {project} {packOptions}", cancellationToken);
            }

            if (noPush)
            {
                Console.WriteLine("Skip push there's noPush specified");
                return;
            }

            if (string.IsNullOrEmpty(apiKey))
            {
                // try to get apiKey from environment variable
                apiKey = Environment.GetEnvironmentVariable("NUGET_API_KEY");

                if (string.IsNullOrEmpty(apiKey))
                {
                    Console.WriteLine("Skip push since there's no apiKey found");
                    return;
                }
            }

            // push nuget packages
            foreach (var file in Directory.GetFiles("./artifacts/packages/", "*.nupkg"))
            {
                await RetryHelper.TryInvokeAsync(() => ExecuteCommandAsync($"dotnet nuget push {file} -s https://api.nuget.org/v3/index.json -k {apiKey} --skip-duplicate", cancellationToken), cancellationToken: cancellationToken);
            }
        }))
    .WithTask("Default", b => WithDependency("pack"))
    .Build()
    .ExecuteAsync(target, ApplicationHelper.ExitToken);

async Task ExecuteCommandAsync(string commandText, CancellationToken cancellationToken = default)
{
    Console.WriteLine($"Executing command: \n    {commandText}");
    Console.WriteLine();

    var result = await CommandExecutor.ExecuteCommandAndOutputAsync(commandText, cancellationToken: cancellationToken);
    result.EnsureSuccessExitCode();
    Console.WriteLine();
}
