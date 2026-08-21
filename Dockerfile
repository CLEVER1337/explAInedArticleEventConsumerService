# user.events → ClickHouse sink (:5259). The HTTP surface is incidental — the work happens in
# the hosted consumer — but the port is still exposed so the container has a liveness target.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# csproj first so a source-only change reuses the restore layer.
COPY explAInedArticleEventConsumerService.csproj ./
RUN dotnet restore explAInedArticleEventConsumerService.csproj

COPY . .
RUN dotnet publish explAInedArticleEventConsumerService.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish ./

ENV ASPNETCORE_HTTP_PORTS=5259
EXPOSE 5259

# The ClickHouse schema is not created by this service — apply schema.sql with clickhouse-client
# before the first run.
USER $APP_UID
ENTRYPOINT ["dotnet", "explAInedArticleEventConsumerService.dll"]
