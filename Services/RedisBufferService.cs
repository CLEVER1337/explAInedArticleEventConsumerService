using Microsoft.Extensions.Caching.Distributed;
using StackExchange.Redis;

public class RedisBufferService
{
    private readonly IDatabase _cache;

    private readonly Logger<RedisBufferService> _logger;

    public RedisBufferService(IConnectionMultiplexer connectionMultiplexer, Logger<RedisBufferService> logger)
    {
        _cache = connectionMultiplexer.GetDatabase();
        _logger = logger;
    }

    public async Task AppendValue(string value)
    {
        await _cache.StringSetAsync(Guid.NewGuid().ToString(), value);

        _logger.LogInformation($"Redis added string: {value}");
    }

    public async Task<List<string>> GetAllValues()
    {
        var keys = _cache.Multiplexer.GetServer(_cache.Multiplexer.GetEndPoints().First()).Keys(pattern: "*");
        var values = new List<string>();

        foreach (var key in keys)
        {
            var value = await _cache.StringGetAsync(key);
            if (value.ToString() != null)
            {
                values.Add(value.ToString());
                await _cache.KeyDeleteAsync(key);
            }
        }

        return values;
    }

    public async Task<int> GetCount()
    {
        var keys = _cache.Multiplexer.GetServer(_cache.Multiplexer.GetEndPoints().First()).Keys(pattern: "*");
        return keys.Count();
    }
}