using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Producer
{
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

                //Um channel para cada produtor - cada channel é uma thread
                var channel1 = CreateChannel(connection, queueName);
                var channel2 = CreateChannel(connection, queueName);
                var channel3 = CreateChannel(connection, queueName);

                BuildPublisher(channel1, queueName, "Produtor A", manualResetEvent);
                BuildPublisher(channel2, queueName, "Produtor B", manualResetEvent);
                BuildPublisher(channel3, queueName, "Produtor C", manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static IModel CreateChannel(IConnection connection, string queueName)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            return channel;
        }

        public static void BuildPublisher(IModel channel, string queue, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                int count = 0;

                do
                {
                    try
                    {
                        string message = $"Ordem Numerica: {count++}";
                        var body = Encoding.UTF8.GetBytes(message);

                        //não deletar mensagem ao reiniciar
                        var basicProps = channel.CreateBasicProperties();
                        basicProps.Persistent = true;

                        channel.BasicPublish("", 
                                             routingKey: queue, 
                                             basicProperties: basicProps, 
                                             body: body);

                        Console.WriteLine($"{publisherName} - [x] Mensagem publicada: {count}", message);
                        Thread.Sleep(1000);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{ex.Message}");

                        manualResetEvent.Set();
                    }

                } while (count < 10);

            });
        }
    }
}
