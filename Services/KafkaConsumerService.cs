using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly string _topic;
    private readonly ConsumerConfig _consumerConfig;

    private readonly RedisBufferService _redisBufferService;

    private readonly ViewedSetService _viewedSetService;

    private readonly ConcurrentQueue<TopicPartitionOffset> _pendingCommits = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IConfiguration configuration,
        RedisBufferService redisBufferService,
        ViewedSetService viewedSetService)
    {
        _logger = logger;
        _topic = configuration["Kafka:Topic"] ?? "default-topic";
        _redisBufferService = redisBufferService;
        _viewedSetService = viewedSetService;

        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = configuration["Kafka:GroupId"] ?? "default-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => StartConsumerLoop(stoppingToken), stoppingToken);
    }

    public void EnqueueCommit(IEnumerable<TopicPartitionOffset> offsets)
    {
        foreach (var offset in offsets)
        {
            _pendingCommits.Enqueue(offset);
        }
    }

    private async Task StartConsumerLoop(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
        consumer.Subscribe(_topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var msg = consumer.Consume(TimeSpan.FromSeconds(1));

                    if(msg != null && !msg.IsPartitionEOF)
                    {
                        _logger.LogInformation($"Consumed message '{msg.Message.Value}' at: '{msg.TopicPartitionOffset}'.");

                        try
                        {
                            var userEvent = string.IsNullOrWhiteSpace(msg.Message.Value)
                                ? null
                                : JsonSerializer.Deserialize<UserEvent>(msg.Message.Value, JsonOptions);

                            if (userEvent != null)
                            {
                                var buffered = new BufferedEvent(
                                    msg.Topic,
                                    msg.Partition.Value,
                                    msg.Offset.Value,
                                    msg.Message.Value);

                                await _redisBufferService.AppendValue(JsonSerializer.Serialize(buffered, JsonOptions));

                                await _viewedSetService.RecordAsync(userEvent);
                            }
                            else
                            {
                                _logger.LogWarning($"Received null UserEvent after deserialization, skipping: '{msg.TopicPartitionOffset}'.");
                            }
                        }
                        catch (JsonException jsonEx)
                        {
                            _logger.LogError($"JSON deserialization error at '{msg.TopicPartitionOffset}': {jsonEx.Message}");
                        }
                    }

                    FlushPendingCommits(consumer);
                }
                catch(ConsumeException ex)
                {
                    _logger.LogError($"Error consuming message: {ex.Error.Reason}");
                }
                catch(KafkaException ex)
                {
                    _logger.LogError($"Kafka error: {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer loop canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Unexpected error in consumer loop: {ex.Message}");
        }
        finally
        {
            FlushPendingCommits(consumer);
            consumer.Close();
        }
    }

    private void FlushPendingCommits(IConsumer<Ignore, string> consumer)
    {
        var offsets = new List<TopicPartitionOffset>();

        while (_pendingCommits.TryDequeue(out var offset))
        {
            offsets.Add(offset);
        }

        if (offsets.Count == 0)
        {
            return;
        }

        try
        {
            consumer.Commit(offsets);
            _logger.LogInformation($"Committed offsets: {string.Join(", ", offsets)}.");
        }
        catch (KafkaException ex)
        {
            _logger.LogError($"Error committing offsets: {ex.Error.Reason}");
        }
    }
}
