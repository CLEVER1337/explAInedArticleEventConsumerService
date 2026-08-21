using StackExchange.Redis;

public class RedisBufferService
{
    private readonly IDatabase _cache;

    private readonly ILogger<RedisBufferService> _logger;

    private readonly RedisKey _bufferKey;

    public RedisBufferService(
        IConnectionMultiplexer connectionMultiplexer,
        IConfiguration configuration,
        ILogger<RedisBufferService> logger)
    {
        _cache = connectionMultiplexer.GetDatabase();
        _logger = logger;
        _bufferKey = configuration["Redis:BufferKey"] ?? "explAIned:user-events:buffer";
    }

    public async Task AppendValue(string value)
    {
        var length = await _cache.ListRightPushAsync(_bufferKey, value);

        _logger.LogDebug($"Redis buffer appended, length now: {length}");
    }

    public async Task<List<string>> PeekValues(int count)
    {
        if (count <= 0)
        {
            return [];
        }

        var values = await _cache.ListRangeAsync(_bufferKey, 0, count - 1);

        return values.Select(v => v.ToString()).Where(v => v is not null).ToList();
    }

    public async Task RemoveProcessed(int count)
    {
        if (count <= 0)
        {
            return;
        }

        await _cache.ListTrimAsync(_bufferKey, count, -1);

        _logger.LogDebug($"Redis buffer trimmed by: {count}");
    }

    public async Task<long> GetCount()
    {
        return await _cache.ListLengthAsync(_bufferKey);
    }
}
