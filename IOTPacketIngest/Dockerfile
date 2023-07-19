FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /App

# Copy everything
COPY . ./
# Restore as distinct layers
RUN dotnet restore ./IOTPacketIngest.sln
# Build and publish a release
RUN dotnet publish ./IOTPacketIngest.sln -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /App
COPY --from=build-env /App/out .
ENTRYPOINT ["dotnet", "IOTPacketIngestParser.dll"]
#ENTRYPOINT ["out"]