using Producer_Direct.Models;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Producer_Header
{
    /// <summary>
    /// HEADER - Pode distribuir de acordo com a chave e valor do header, não olha para RoutingKey
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

            channel.QueueDeclare(queue: "comercial", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "suporte", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "desenvolvimento", durable: false, exclusive: false, autoDelete: false, arguments: null);

            channel.ExchangeDeclare("Exchange-Teste-Headers", ExchangeType.Headers);

            var dic = new Dictionary<string, object>();
            dic.Add("setor", "suporte");

            channel.QueueBind("suporte", "Exchange-Teste-Headers", "", dic);

            return channel;
        }

        public static void BuildAndRunPublishers(IModel channel, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                while (true)
                {
                    try
                    {
                        byte[] messageBodyBytes = Encoding.UTF8.GetBytes("Mensagem para Suporte Header!");

                        IBasicProperties props = channel.CreateBasicProperties();
                        props.ContentType = "text/plain";
                        props.DeliveryMode = 2;
                        props.Headers = new Dictionary<string, object>();
                        props.Headers.Add("setor", "suporte");

                        channel.BasicPublish("Exchange-Teste-Headers",
                                             "",
                                             props,
                                             messageBodyBytes);

                        Console.WriteLine("Enviado");
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
