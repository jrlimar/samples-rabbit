using Producer_Direct.Models;
using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Producer_Direct
{
    /// <summary>
    /// DIRECT - Distribui mensagem pra outras filas se tiver a mesma ROUTINGKEY
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
                var queueName = "order";

                var channel = CreateChannel(connection);

                BuildAndRunPublishers(channel, queueName, "Produtor A", manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static IModel CreateChannel(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "order", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "finance_orders", durable: false, exclusive: false, autoDelete: false, arguments: null);

            channel.ExchangeDeclare("order", ExchangeType.Direct);

            //quando publicar a mensagem vai cair em 2 filas, pois tem a mesma ROUTINGKEY
            channel.QueueBind("order", "order", "order_new");
            channel.QueueBind("finance_orders", "order", "order_new");

            //somente em 1 fila
            channel.QueueBind("order", "order", "order_upd");
           

            return channel;
        }

        public static void BuildAndRunPublishers(IModel channel, string queue, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                var idIndex = 1;
                var randon = new Random(DateTime.UtcNow.Millisecond * DateTime.UtcNow.Second);

                while (true)
                {
                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir mais mansagens");
                        Console.ReadLine();

                        var order = new Order(idIndex++, randon.Next(1000, 9999));
                        var message1 = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));

                        //vai publicar em 2 filas com RoutingKey "order_new"
                        channel.BasicPublish("order", "order_new", null, message1);
                        Console.WriteLine($"New order Id {order.Id}: Amount {order.Amount} | Created: {order.CreateDate:o}");

                        order.UpdateOrder(randon.Next(100, 999));
                        var message2 = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));

                        channel.BasicPublish("order", "order_upd", null, message2);
                        Console.WriteLine($"Upd Id {order.Id}: Amount {order.Amount} | LastUpdated: {order.LastUpdated:o}");
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
