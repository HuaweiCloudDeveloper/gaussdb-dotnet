#!/bin/sh

dotnet tool update -g dotnet-execute
export PATH="$PATH:$HOME/.dotnet/tools"

echo "dotnet-exec ./build.cs --args $@"
dotnet-exec ./build.cs --args "$@"