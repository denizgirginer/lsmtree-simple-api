using LsmStoreApi.LsmStore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// LsmTreeManager.StoreTest.LoadSSTables();

/*
var ssTable = new SSTable("deneme-test1");
ssTable.Delete();
ssTable.Write(new Dictionary<string, string>() {
    { "key1", "11111"},
    { "key2", "22222"},
    { "key3", "33333x"},
    { "key4", "44444zzz"},
    { "key5", "55555"},
});

ssTable.LoadIndex();

await Task.Delay(100);

var ssTable2 = new SSTable("deneme-test2");
ssTable2.Delete();
ssTable2.Write(new Dictionary<string, string>() {
    { "key2", "99999a"},
    { "key3", "yxxxxxy"},
});

await Task.Delay(1400);

var ssTable3 = new SSTable("deneme-test3");
ssTable3.Delete();
ssTable3.Write(new Dictionary<string, string>() {
    { "key1", "AAAAA"},
    { "key4", "ZZZZZ"},    
});

await Task.Delay(100);


var valueFirst = ssTable.GetFirstKey();


var valueNext = ssTable.GetNextValue(valueFirst);

var value = ssTable.GetValue("key4");

var value2 = ssTable.GetValue("key3");
*/


LsmTreeManager.StoreTest.Init();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
