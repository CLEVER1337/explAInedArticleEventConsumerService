using StackExchange.Redis;
using ClickHouse.Driver;

var builder = WebApplication.CreateBuilder(args);

// json config
builder.Configuration.AddJsonFile("appsettings.json");

builder.Services.AddSingleton<IConnectionMultiplexer>(sp => 
    ConnectionMultiplexer.Connect(builder.Configuration["ConnectionStrings:Redis"]));

builder.Services.AddSingleton(new ClickHouseClient(builder.Configuration["ConnectionStrings:ClickHouse"]));

// Add services to the container.
builder.Services.AddControllersWithViews();

builder.Services.AddSingleton<RedisBufferService>();

builder.Services.AddHostedService<KafkaConsumerService>();

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
