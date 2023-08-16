FROM mcr.microsoft.com/dotnet/sdk:7.0.306-jammy-amd64 AS build-env
WORKDIR /App

# Copy everything
COPY . ./
# Restore as distinct layers
RUN dotnet restore

# Add Assemblies to Image
RUN dotnet add package AWSSDK.Extensions.NETCore.Setup
RUN dotnet add package AWSSDK.S3
RUN dotnet add package AWSSDK.SecurityToken
RUN dotnet add package Grpc.AspNetCore
RUN dotnet add package Grpc.Tools

# Build and publish a release
RUN dotnet publish -c Release -o out /property:PublishWithAspNetCoreTargetManifest=false

# Build runtime image
FROM mcr.microsoft.com/dotnet/sdk:7.0.306-jammy-amd64
WORKDIR /App
COPY --from=build-env /App/out .
RUN mkdir -p /var/run/secrets/eks.amazonaws.com/serviceaccount/
ENTRYPOINT ["dotnet", "IOTPacketIngestParser.dll"]
#ENTRYPOINT ["out"]