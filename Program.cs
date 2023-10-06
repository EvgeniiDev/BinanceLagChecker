using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;

Console.WriteLine("Starting");
var pairs = new List<string> {"ethusdt", "btcusdt", "xrpusdt"};

var uri = new Uri($"wss://stream.binance.com:443/stream?" +
                  $"streams={string.Join('/', pairs.Select(x => $"{x.ToLowerInvariant()}@kline_1m"))}");
using var ws = new ClientWebSocket();

await ws.ConnectAsync(uri, CancellationToken.None);
var buf = new byte[2000];

while (ws.State == WebSocketState.Open)
{
    var result = await ws.ReceiveAsync(buf, CancellationToken.None);

    if (result.MessageType == WebSocketMessageType.Close)
    {
        await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        Console.WriteLine(result.CloseStatusDescription);
    }
    else
    {
        var obj = JsonSerializer.Deserialize<Root>(buf.AsSpan()[..result.Count]);
        var x = obj.Data.Details;

        if (!x.IsCandleClosed) continue;

        var candleTime = obj.Data.EventTime - (obj.Data.EventTime % (60 * 1000));

        var currentTime = DateTime.UtcNow;
        var lag = currentTime.ToMilliseconds() - candleTime;
        
        Console.WriteLine($"{lag}  {currentTime.ToString("O")}");
    }
}


Console.WriteLine("end");


public static class DateTimeExtension
{
    private static readonly DateTime Jan1St1970 = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static long ToMilliseconds(this DateTime dt)
    {
        return (dt - Jan1St1970).Ticks / TimeSpan.TicksPerMillisecond;
    }

    public static long ToUtc(this DateTime dt)
    {
        return (long) (dt - Jan1St1970).TotalSeconds;
    }

    public static DateTime ToDateTime(this long milliseconds)
    {
        return Jan1St1970.AddMilliseconds(milliseconds);
    }
}

public class Root
{
    [JsonPropertyName("stream")] public string Stream { get; set; }

    [JsonPropertyName("data")] public CandlestickData Data { get; set; }
}

public class CandlestickData
{
    [JsonPropertyName("e")] public string EventType { get; set; }

    [JsonPropertyName("E")] public long EventTime { get; set; }

    [JsonPropertyName("s")] public string Symbol { get; set; }

    [JsonPropertyName("k")] public CandlestickDetails Details { get; set; }
}

public class CandlestickDetails
{
    [JsonPropertyName("t")] public long StartTime { get; set; }

    [JsonPropertyName("T")] public long EndTime { get; set; }

    [JsonPropertyName("s")] public string Symbol { get; set; }

    [JsonPropertyName("i")] public string Interval { get; set; }

    [JsonPropertyName("f")] public long FirstTradeId { get; set; }

    [JsonPropertyName("L")] public long LastTradeId { get; set; }

    [JsonPropertyName("o")] public string OpenPrice { get; set; }

    [JsonPropertyName("c")] public string ClosePrice { get; set; }

    [JsonPropertyName("h")] public string HighPrice { get; set; }

    [JsonPropertyName("l")] public string LowPrice { get; set; }

    [JsonPropertyName("v")] public string TradeVolume { get; set; }

    [JsonPropertyName("n")] public int TradeCount { get; set; }

    [JsonPropertyName("x")] public bool IsCandleClosed { get; set; }

    [JsonPropertyName("q")] public string QuotedTradeVolume { get; set; }

    [JsonPropertyName("V")] public string TakerBuyBaseAssetVolume { get; set; }

    [JsonPropertyName("Q")] public string TakerBuyQuoteAssetVolume { get; set; }

    [JsonPropertyName("B")] public string UndefinedFieldB { get; set; }
}