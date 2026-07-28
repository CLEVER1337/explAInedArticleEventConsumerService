using Confluent.Kafka;
using System.Text.Json;
using System.Text.Json.Serialization;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly string _topic;
    private readonly ConsumerConfig _consumerConfig;

    private readonly RedisBufferService _redisBufferService;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger, IConfiguration configuration, RedisBufferService redisBufferService)
    {
        _logger = logger;
        _topic = configuration["Kafka:Topic"] ?? "default-topic";
        _redisBufferService = redisBufferService;

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
                    var msg = consumer.Consume(stoppingToken);

                    if(msg != null)
                    {
                        _logger.LogInformation($"Consumed message '{msg.Message.Value}' at: '{msg.TopicPartitionOffset}'.");

                        try
                        {
                            // var userEvent = JsonSerializer.Deserialize<UserEvent>(msg.Message.Value, JsonOptions);
                            // if (userEvent != null)
                            // {
                            //     _logger.LogInformation($"Deserialized UserEvent: {userEvent}");
                                
                            //     // business logic here


                            // }
                            // else
                            // {
                            //     _logger.LogWarning("Received null UserEvent after deserialization.");
                            // }

                            await _redisBufferService.AppendValue(msg.Message.Value);
                        }
                        catch (JsonException jsonEx)
                        {
                            _logger.LogError($"JSON deserialization error: {jsonEx.Message}");
                        }
                    }
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
            consumer.Close();
        }
    }

    public async void ConsumerCommitMessage(ConsumeResult<Ignore, string> msg)
    {
        try
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
            consumer.Commit(msg);
            _logger.LogInformation($"Committed message at: '{msg.TopicPartitionOffset}'.");
        }
        catch (KafkaException ex) 
        {
            _logger.LogError($"Error committing message: {ex.Error.Reason}");
        }
    }
}