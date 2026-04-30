#!/bin/sh

set -eu

dotnet tool restore

echo "dotnet tool run dotnet-exec ./build.cs --args $*"
dotnet tool run dotnet-exec ./build.cs --args "$@"
