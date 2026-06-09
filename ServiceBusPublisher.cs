using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

public class ServiceBusPublisher
{
    private readonly IConfiguration _configuration;
    private ServiceBusSender? _sender;

    public ServiceBusPublisher(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    private async Task<ServiceBusSender> GetSenderAsync()
    {
        if (_sender != null)
        {
            return _sender;
        }

        var vaultUrl = _configuration["KeyVaultUrl"];

        var secretClient = new SecretClient(new Uri(vaultUrl), new DefaultAzureCredential());

        var secret = await secretClient.GetSecretAsync("ServiceBusConnectionString");

        var connectionString = secret.Value.Value;

        var client = new ServiceBusClient(connectionString);

        _sender = client.CreateSender("csv-queue");

        return _sender;
    }

    public async Task SendAsync(string json)
    {
        var sender = await GetSenderAsync();

        await sender.SendMessageAsync(
            new ServiceBusMessage(json));
    }
}