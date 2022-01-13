using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Producer_Exchanges
{
    /// <summary>
    /// FANOUT - Distribui mensagem em várias filas
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

                BuildPublisher(channel, queueName, "Produtor", manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static IModel CreateChannel(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "teste-fanaout", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "logs", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "financeiro", durable: false, exclusive: false, autoDelete: false, arguments: null);

            channel.ExchangeDeclare("distribuidor", ExchangeType.Fanout); //publico no exchange e ele faz a distribuição

            channel.QueueBind("teste-fanaout", "distribuidor", ""); // aqui eu digo que essa fila está ligada e tal exchange
            channel.QueueBind("logs", "distribuidor", ""); // aqui eu digo que essa fila está ligada e tal exchange
            channel.QueueBind("financeiro", "distribuidor", ""); // aqui eu digo que essa fila está ligada e tal exchange

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

                        channel.BasicPublish("distribuidor"/*uso exchange*/, "", basicProperties: null, body: body);

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
