using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class ServiceBusPublisher
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ServiceBusPublisher> _logger;
    private ServiceBusSender? _sender;

    public ServiceBusPublisher(
        IConfiguration configuration,
        ILogger<ServiceBusPublisher> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    private async Task<ServiceBusSender> GetSenderAsync()
    {
        if (_sender != null)
        {
            _logger.LogInformation("Reusing existing Service Bus sender.");
            return _sender;
        }

        try
        {
            var vaultUrl = _configuration["KeyVaultUrl"];

            _logger.LogInformation("KeyVaultUrl: {VaultUrl}", vaultUrl);

            _logger.LogInformation("Creating SecretClient...");

            var secretClient = new SecretClient(
                new Uri(vaultUrl),
                new DefaultAzureCredential());

            _logger.LogInformation("Retrieving ServiceBusConnectionString secret...");

            var secret = await secretClient.GetSecretAsync(
                "ServiceBusConnectionString");

            _logger.LogInformation("Successfully retrieved secret from Key Vault.");

            var connectionString = secret.Value.Value;

            _logger.LogInformation("Creating Service Bus client...");

            var client = new ServiceBusClient(connectionString);

            _logger.LogInformation("Creating sender for queue: csv-queue");

            _sender = client.CreateSender("csv-queue");

            _logger.LogInformation("Service Bus sender created successfully.");

            return _sender;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error while creating Service Bus sender.");

            throw;
        }
    }

    public async Task SendAsync(string json)
    {
        try
        {
            _logger.LogInformation("Preparing to send message: {Message}", json);

            var sender = await GetSenderAsync();

            await sender.SendMessageAsync(
                new ServiceBusMessage(json));

            _logger.LogInformation("Message sent successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error sending message to Service Bus.");

            throw;
        }
    }
}