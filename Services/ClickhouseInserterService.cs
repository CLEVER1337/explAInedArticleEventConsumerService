public class ClickhouseInserterService : BackgroundService
{
    private readonly ILogger<ClickhouseInserterService> _logger;
    private readonly RedisBufferService _redisBufferService;
    private readonly int _batchSize;
    private readonly int _insertDelayMs;
    private DateTime _lastInsertTime;

    public ClickhouseInserterService(ILogger<ClickhouseInserterService> logger, RedisBufferService redisBufferService, IConfiguration configuration)
    {
        _logger = logger;
        _redisBufferService = redisBufferService;
        _batchSize = int.Parse(configuration["ClickHouseBatchSize"] ?? "100");
        _insertDelayMs = int.Parse(configuration["ClickhouseInsertDelayMs"] ?? "10000");
        _lastInsertTime = DateTime.MinValue;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var values = await _redisBufferService.GetAllValues();

                if (values.Count > _batchSize || (DateTime.UtcNow - _lastInsertTime).TotalMilliseconds >= _insertDelayMs)
                {
                    // Insert values into ClickHouse here
                    _logger.LogInformation($"Inserting {values.Count} values into ClickHouse.");
                    _lastInsertTime = DateTime.UtcNow;
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