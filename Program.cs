using StackExchange.Redis;
using ClickHouse.Driver;

var builder = WebApplication.CreateBuilder(args);

// No AddJsonFile here. CreateBuilder has already loaded appsettings.json, and adding it a
// second time appends it as the *last* configuration provider — above environment variables,
// which are registered before it. Every ConnectionStrings__* and Kafka__* the deployment sets
// was then silently ignored and this service dialled localhost.

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
    ConnectionMultiplexer.Connect(builder.Configuration["ConnectionStrings:Redis"]));

builder.Services.AddSingleton(new ClickHouseClient(builder.Configuration["ConnectionStrings:ClickHouse"]));

// Add services to the container.
builder.Services.AddControllersWithViews();

builder.Services.AddSingleton<RedisBufferService>();
builder.Services.AddSingleton<ViewedSetService>();

// Singleton + resolved-instance hosted service: the inserter hands offsets back to the very
// same consumer, since only the instance holding the partition assignment can commit them.
builder.Services.AddSingleton<KafkaConsumerService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KafkaConsumerService>());

builder.Services.AddHostedService<ClickhouseInserterService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseRouting();

app.UseAuthorization();

app.MapStaticAssets();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}")
    .WithStaticAssets();


app.Run();
