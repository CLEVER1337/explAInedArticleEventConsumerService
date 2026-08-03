using ClickHouse.Driver;
using System.Text.Json;
using System.Text.Json.Serialization;

public class ClickhouseInserterService : BackgroundService
{
    private readonly ILogger<ClickhouseInserterService> _logger;
    private readonly RedisBufferService _redisBufferService;
    private readonly ClickHouseClient _clickHouseClient;
    private readonly int _batchSize;
    private readonly int _insertDelayMs;
    private readonly string _tableName;
    private DateTime _lastInsertTime;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public ClickhouseInserterService(
        ILogger<ClickhouseInserterService> logger, 
        RedisBufferService redisBufferService, 
        ClickHouseClient clickHouseClient, 
        IConfiguration configuration)
    {
        _logger = logger;
        _redisBufferService = redisBufferService;
        _clickHouseClient = clickHouseClient;
        _batchSize = int.Parse(configuration["ClickHouse:BatchSize"] ?? "100");
        _insertDelayMs = int.Parse(configuration["ClickHouse:InsertDelayMs"] ?? "10000");
        _tableName = configuration["ClickHouse:Table"] ?? "article_events";
        _lastInsertTime = DateTime.MinValue;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var values = await _redisBufferService.GetAllValues();

                if (values.Count >= _batchSize || (DateTime.UtcNow - _lastInsertTime).TotalMilliseconds >= _insertDelayMs)
                {
                    // Insert values into ClickHouse here
                    _logger.LogInformation($"Inserting {values.Count} values into ClickHouse.");

                    IEnumerable<object[]> rows = values.Select(v => {
                        var userEvent = JsonSerializer.Deserialize<UserEvent>(v, JsonOptions);
                        return new object[] { userEvent?.EventId, userEvent?.EventType, userEvent?.UserId, userEvent?.ArticleId, userEvent?.OccurredAt, userEvent?.Source, userEvent?.Metadata };
                    });

                    string[] columnNames = JsonSerializer.Deserialize<UserEvent>(values.FirstOrDefault() ?? "{}", JsonOptions)?.GetType().GetProperties().Select(p => p.Name).ToArray() ?? Array.Empty<string>();

                    long rowsInserted = await _clickHouseClient.InsertBinaryAsync(_tableName, columnNames, rows, cancellationToken: stoppingToken);
                    _lastInsertTime = DateTime.UtcNow;

                    _logger.LogInformation($"Inserted {rowsInserted} rows into ClickHouse.");
                }
                else
                {
                    _logger.LogInformation("No values to insert into ClickHouse.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error inserting into ClickHouse: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}