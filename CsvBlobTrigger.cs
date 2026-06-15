using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using CsvHelper;
using System.Globalization;
using System.Linq;
using System.Text.Json;

namespace CsvBlobProcessor;

public class CsvBlobTrigger
{
    private readonly ILogger<CsvBlobTrigger> _logger;
    private readonly ServiceBusPublisher _publisher;

    public CsvBlobTrigger(ILogger<CsvBlobTrigger> logger, ServiceBusPublisher publisher)
    {
        _logger = logger;
        _publisher = publisher;
    }

    [Function(nameof(CsvBlobTrigger))]
    public async Task Run([BlobTrigger("uploads/{name}", Connection = "AzureWebJobsStorage")] Stream stream, string name)
    {
        using var blobStreamReader = new StreamReader(stream);
        var content = await blobStreamReader.ReadToEndAsync();
        _logger.LogInformation("C# Blob trigger function Processed blob\n Name: {name} \n Data: {content}", name, content);

        using var reader = new StreamReader(stream);

        stream.Position = 0;

        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        var records = csv.GetRecords<dynamic>().ToList();

        foreach (var record in records)
        {
            try
            {
                var json = JsonSerializer.Serialize(record);

                _logger.LogInformation("Sending: {Json}", json);

                await _publisher.SendAsync(json);

                _logger.LogInformation("Successfully sent");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending Service Bus message");
                throw;
            }
        }
    }
}