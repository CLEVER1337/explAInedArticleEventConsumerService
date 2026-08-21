using StackExchange.Redis;

public class ViewedSetService
{
    private static readonly string[] ViewedEventTypes = ["ArticleRead", "ArticleClicked"];

    private readonly IDatabase _cache;
    private readonly ILogger<ViewedSetService> _logger;
    private readonly TimeSpan _ttl;

    public ViewedSetService(
        IConnectionMultiplexer connectionMultiplexer,
        IConfiguration configuration,
        ILogger<ViewedSetService> logger)
    {
        _cache = connectionMultiplexer.GetDatabase();
        _logger = logger;
        _ttl = TimeSpan.FromDays(int.TryParse(configuration["Redis:ViewedTtlDays"], out var days) ? days : 30);
    }

    public static bool Tracks(string eventType) => ViewedEventTypes.Contains(eventType);

    public async Task RecordAsync(UserEvent userEvent)
    {
        if (!Tracks(userEvent.EventType)) return;
        if (string.IsNullOrEmpty(userEvent.UserId) || string.IsNullOrEmpty(userEvent.ArticleId)) return;

        try
        {
            RedisKey key = $"rec:user_viewed:{userEvent.UserId}";

            await _cache.SetAddAsync(key, userEvent.ArticleId);
            await _cache.KeyExpireAsync(key, _ttl);
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Failed to record viewed article for user {userEvent.UserId}: {ex.Message}");
        }
    }
}
