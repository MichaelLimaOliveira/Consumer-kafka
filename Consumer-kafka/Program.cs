using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:29092",
    GroupId = "creditengine",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoOffsetStore = false
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("teste-kafka");

CancellationTokenSource token = new();

try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);
        if (response.Message != null)
        {
            var weather = JsonConvert.DeserializeObject<Weather>
                (response.Message.Value);
            Console.WriteLine($"State: {weather.State}, " +
                $"Temp: {weather.Temparature}F");
        }
    }
}
catch (Exception)
{

    throw;
}

public record Weather(string State, int Temparature);