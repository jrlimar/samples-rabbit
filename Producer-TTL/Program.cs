using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Producer_TTL
{
    /// <summary>
    /// TTL tempo de duração da mensagem, se nunguém consumir ela sai da fla
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
                var queueName = "teste-TTL";
                var channel = connection.CreateModel();

                //Forma 1 - tempo de expiração 10 segundos 
                var props = channel.CreateBasicProperties();
                props.Expiration = "20000";

                //forma 2 "x-message-ttl" - seria mais se fosse um tempo padrão da fila
                var arguments = new Dictionary<string, object>();
                arguments.Add("x-message-ttl", 20000);

                channel.QueueDeclare(queue: queueName, 
                                     durable: false, 
                                     exclusive: false, 
                                     autoDelete: false, 
                                     arguments: arguments/*forma 2*/);

                BuildPublisher(channel, queueName, "Produtor A", props/*forma 1*/, manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static void BuildPublisher(IModel channel, string queue, string publisherName, IBasicProperties basicProperties, ManualResetEvent manualResetEvent)
        {
            try
            {
                var message = $"Tempo de Expiração: {10} segundos";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("",
                                     routingKey: queue,
                                     basicProperties: basicProperties,
                                     body: body);

                Console.WriteLine($"{publisherName} - [x] Mensagem publicada", message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message}");
                manualResetEvent.Set();
            }
        }
    }
}
