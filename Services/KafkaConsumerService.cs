using Confluent.Kafka;
using System.Text.Json;
using System.Text.Json.Serialization;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly string _topic;
    private readonly ConsumerConfig _consumerConfig;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _topic = configuration["Kafka:Topic"] ?? "default-topic";

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

    private void StartConsumerLoop(CancellationToken stoppingToken)
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
                            var userEvent = JsonSerializer.Deserialize<UserEvent>(msg.Message.Value, JsonOptions);
                            if (userEvent != null)
                            {
                                _logger.LogInformation($"Deserialized UserEvent: {userEvent}");
                                
                                // business logic here

                                consumer.Commit(msg);
                            }
                            else
                            {
                                _logger.LogWarning("Received null UserEvent after deserialization.");
                            }
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
}