using Producer_Direct.Models;
using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Producer_Topic
{
    /// <summary>
    /// TOPIC - Regras de envio de mensagem de acordo com RoutingKey 
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            var manualResetEvent = new ManualResetEvent(false);

            manualResetEvent.Reset();

            using (var connection = factory.CreateConnection())
            {
                var channel = CreateChannel(connection);

                BuildAndRunPublishers(channel, manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static IModel CreateChannel(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "pixeon", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "diretoria", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "comercial", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "desenvolvimento", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "suporte", durable: false, exclusive: false, autoDelete: false, arguments: null);

            channel.ExchangeDeclare("Exchange-Email", ExchangeType.Topic);

            channel.QueueBind("pixeon", "Exchange-Email", "pixeon");
            channel.QueueBind("diretoria", "Exchange-Email", "pixeon.diretoria");
            channel.QueueBind("comercial", "Exchange-Email", "pixeon.comercial");
            channel.QueueBind("desenvolvimento", "Exchange-Email", "pixeon.desenvolvimento");
            channel.QueueBind("suporte", "Exchange-Email", "pixeon.suporte");

            //Regra - Mensagem chega por hierarquia - Exemplo sempre a fila "pixeon" recebe email quando manda pra fila de outras areas
            channel.QueueBind("pixeon", "Exchange-Email", "pixeon.*");

            return channel;
        }

        public static void BuildAndRunPublishers(IModel channel, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                var idIndex = 1;
                var randon = new Random(DateTime.UtcNow.Millisecond * DateTime.UtcNow.Second);

                while (true)
                {
                    try
                    {
                        var order = new Order(idIndex++, randon.Next(1000, 9999));
                        var message1 = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));

                        channel.BasicPublish("Exchange-Email",
                                             "pixeon.suporte", // "pixeon.suporte"(fila pixeon e pixeon.suporte recebem) ou "pixeon"(somente fila pixeon recebe)
                                              null, 
                                              message1);

                        Console.WriteLine($"Envio Mensagem Id {order.Id}: Amount {order.Amount} | Created: {order.CreateDate:o}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        manualResetEvent.Set();
                    }
                }
            });
        }
    }
}
