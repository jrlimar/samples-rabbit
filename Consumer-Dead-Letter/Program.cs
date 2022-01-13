using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace Consumer_Dead_Letter
{
    /// <summary>
    /// Estrategia para não deixar em lopping caso mensagem de erro, ele entrega a mensagem para fila de dead letter 
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //Estrategia Dead Letter
                channel.ExchangeDeclare("Teste-Exchance-Dead-Letter", ExchangeType.Fanout);
                channel.QueueDeclare(queue: "dead-letter-queue", durable: true, exclusive: false, autoDelete: false,  arguments: null);
                channel.QueueBind("dead-letter-queue", "Teste-Exchance-Dead-Letter", "");

                var arguments = new Dictionary<string, object> 
                {
                    { "x-dead-letter-exchange", "Teste-Exchance-Dead-Letter"}  //Palavra reservada RabbitMQ: x-dead-letter-exchange
                };
                //

                channel.QueueDeclare(queue: "taks-dead", 
                                     durable: true, 
                                     exclusive: false, 
                                     autoDelete: false, 
                                     arguments: arguments);

                channel.BasicQos(0, 1, false);

                BuildConsumer(channel, "Worker 1");

                Console.ReadLine();
            }
        }

        public static void BuildConsumer(IModel channel, string consumerName)
        {
            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var result = int.Parse(message);

                    Console.WriteLine($"{consumerName} - Recebido (Usando dead letter, caso der erro não fica em looping)- {result}");

                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ex.Message}");

                    channel.BasicNack(ea.DeliveryTag, false, false/*false não colocar de volta na fila*/);
                }
            };

            channel.BasicConsume(queue: "taks-dead", 
                                 autoAck: false, 
                                 consumer: consumer);
        }
    }
}
