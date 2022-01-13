using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "order", durable: false, exclusive: false, autoDelete: false, arguments: null);

                //Pode compartilhar o mesmo canal - mas verificar a carga pois se um processo de um worker demorar o outro vai aguardar terminar(solução não compratilhar o channel)
                BuildConsumer(channel, "Worker 1");
                BuildConsumer(channel, "Worker 2");
                BuildConsumer(channel, "Worker 3");

                Console.ReadLine();
            }
        }

        public static void BuildConsumer(IModel channel, string consumerName)
        {
            var consumer = new EventingBasicConsumer(channel);

            channel.BasicQos(0, 1/*pega uma mensagem de cada vez*/, false/*false por consumer || true por channel*/);

            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"{consumerName} - Recebido - {message}");

                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ex.Message}");

                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
            };

            channel.BasicConsume(queue: "order", autoAck: false, consumer: consumer);
        }
    }
}
