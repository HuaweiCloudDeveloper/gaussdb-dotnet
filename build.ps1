dotnet tool update -g dotnet-execute

Write-Host 'dotnet-exec ./build.cs "--args=$ARGS"' -ForegroundColor GREEN
 
dotnet-exec ./build/build.cs --args $ARGS