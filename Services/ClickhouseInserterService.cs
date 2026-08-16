using ClickHouse.Driver;
using Confluent.Kafka;
using System.Text.Json;
using System.Text.Json.Serialization;

public class ClickhouseInserterService : BackgroundService
{
    private readonly ILogger<ClickhouseInserterService> _logger;
    private readonly RedisBufferService _redisBufferService;
    private readonly KafkaConsumerService _kafkaConsumerService;
    private readonly ClickHouseClient _clickHouseClient;
    private readonly int _batchSize;
    private readonly int _insertDelayMs;
    private readonly string _tableName;
    private DateTime _lastInsertTime;

    // Must match the table DDL — see the schema in CONTRACT.md.
    private static readonly string[] ColumnNames =
        ["event_id", "event_type", "user_id", "article_id", "occurred_at", "source", "metadata"];

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public ClickhouseInserterService(
        ILogger<ClickhouseInserterService> logger,
        RedisBufferService redisBufferService,
        KafkaConsumerService kafkaConsumerService,
        ClickHouseClient clickHouseClient,
        IConfiguration configuration)
    {
        _logger = logger;
        _redisBufferService = redisBufferService;
        _kafkaConsumerService = kafkaConsumerService;
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
                var buffered = await ReadDueBatch();

                if (buffered.Count > 0)
                {
                    var events = ParseBufferedEvents(buffered);
                    var rows = BuildRows(events);

                    if (rows.Count > 0)
                    {
                        _logger.LogInformation($"Inserting {rows.Count} values into ClickHouse.");

                        long rowsInserted = await _clickHouseClient.InsertBinaryAsync(_tableName, ColumnNames, rows, cancellationToken: stoppingToken);

                        _logger.LogInformation($"Inserted {rowsInserted} rows into ClickHouse.");
                    }

                    _lastInsertTime = DateTime.UtcNow;

                    await _redisBufferService.RemoveProcessed(buffered.Count);

                    _kafkaConsumerService.EnqueueCommit(BuildCommitOffsets(events));
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

    private async Task<List<string>> ReadDueBatch()
    {
        var count = await _redisBufferService.GetCount();

        if (count == 0)
        {
            return [];
        }

        var due = count >= _batchSize || (DateTime.UtcNow - _lastInsertTime).TotalMilliseconds >= _insertDelayMs;

        return due ? await _redisBufferService.PeekValues(_batchSize) : [];
    }

    private List<BufferedEvent> ParseBufferedEvents(List<string> buffered)
    {
        var events = new List<BufferedEvent>(buffered.Count);

        foreach (var value in buffered)
        {
            try
            {
                var bufferedEvent = JsonSerializer.Deserialize<BufferedEvent>(value, JsonOptions);

                if (bufferedEvent != null)
                {
                    events.Add(bufferedEvent);
                }
                else
                {
                    _logger.LogWarning($"Skipping null buffer entry: {value}");
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError($"Malformed buffer entry, skipping: {ex.Message}");
            }
        }

        return events;
    }

    private List<object[]> BuildRows(List<BufferedEvent> events)
    {
        var rows = new List<object[]>(events.Count);

        foreach (var bufferedEvent in events)
        {
            UserEvent? userEvent;

            try
            {
                userEvent = JsonSerializer.Deserialize<UserEvent>(bufferedEvent.Payload, JsonOptions);
            }
            catch (JsonException ex)
            {
                _logger.LogError($"Malformed payload at offset {bufferedEvent.Offset}, skipping: {ex.Message}");
                continue;
            }

            if (userEvent == null || !Guid.TryParse(userEvent.EventId, out var eventId))
            {
                _logger.LogWarning($"Unusable event at offset {bufferedEvent.Offset}, skipping.");
                continue;
            }

            var metadata = userEvent.Metadata?.ToDictionary(pair => pair.Key, pair => pair.Value?.ToString() ?? string.Empty)
                ?? [];

            rows.Add([
                eventId,
                userEvent.EventType,
                userEvent.UserId,
                userEvent.ArticleId,
                userEvent.OccurredAt,
                userEvent.Source,
                metadata,
            ]);
        }

        return rows;
    }

    private static List<TopicPartitionOffset> BuildCommitOffsets(List<BufferedEvent> events)
    {
        return events
            .GroupBy(bufferedEvent => (bufferedEvent.Topic, bufferedEvent.Partition))
            .Select(partition => new TopicPartitionOffset(
                partition.Key.Topic,
                new Partition(partition.Key.Partition),
                new Offset(partition.Max(bufferedEvent => bufferedEvent.Offset) + 1)))
            .ToList();
    }
}
