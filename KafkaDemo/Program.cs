using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

namespace KafkaDemo
{
    class Program
    {
        // const string BootstrapServers = "192.168.199.133:9092";
        const string BootstrapServers = "192.168.199.133:9093,192.168.199.133:9094,192.168.199.133:9095";
        // const string GroupId = "demo-group";
        const string GroupId = "pps-rc-group";
        // const string Topic = "demo-topic";
        const string Topic = "pps-rc-topic";

        static void Main(string[] args)
        {
            // Task.WaitAll(ProducerDemoAsync(), ConsumerDemoAsync());
            // Task.WaitAll(ProducerDemoAsync());
            // Task.WaitAll(ConsumerDemoAsync());
            // Task.WaitAll(ConsumerDemoAsync(), ConsumerDemoAsync(), ConsumerDemoAsync());
            // Task.WaitAll(MultiConsumerAsync(3), MultiProducerAsync(3));
            // Task.WaitAll(MultiConsumerAsync(3));
            // Task.WaitAll(MultiProducerAsync(50));
        }

        static async Task MultiConsumerAsync(int num)
        {
            if (num <= 0) return;
            Task[] tasks = new Task[num];
            for (int n = 0; n < num; n++)
            {
                tasks[n] = ConsumerDemoAsync();
            }
            await Task.WhenAll(tasks);
        }

        static async Task MultiProducerAsync(int num)
        {
            if (num <= 0) return;
            Task[] tasks = new Task[num];
            for (int n = 0; n < num; n++)
            {
                tasks[n] = ProducerDemoAsync();
            }
            await Task.WhenAll(tasks);
        }

        static async Task ConsumerDemoAsync()
        {
            ConsumerConfig consumerConfig = new()
            {
                // BootstrapServers = "192.168.199.133:9093,192.168.199.133:9094,192.168.199.133:9095",
                ClientId = Guid.NewGuid().ToString("n"),
                BootstrapServers = BootstrapServers,
                GroupId = GroupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = false,
                // PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                SessionTimeoutMs = 6000,
                MaxPollIntervalMs = 600000,
            };

            IConsumer<string, string> consumer =
                new ConsumerBuilder<string, string>(consumerConfig)
                    .SetErrorHandler((msg, e) =>
                    {
                        Console.WriteLine($"Error: {e.Reason}");
                    })
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                    }).Build();

            consumer.Subscribe(Topic);

            await Task.Factory.StartNew(() =>
            {
                try
                {
                    Random random = new(Environment.TickCount);
                    while (true)
                    {
                        var consumeResult = consumer.Consume();
                        Console.WriteLine($"Consumer::{consumeResult.Message?.Key}::{consumeResult.Message?.Value}::{consumeResult.Partition.Value}::{consumeResult.Offset.Value}::{Thread.CurrentThread.ManagedThreadId}");
                        // Thread.Sleep(1000);
                        if (random.Next(0, 2) == 0)
                        {
                            consumer.Commit(consumeResult);
                            Console.WriteLine($"Commit: {consumeResult.Message?.Key}");
                        }
                        else
                        {
                            Console.WriteLine($"Uncommit: {consumeResult.Message?.Key}");
                        }
                    }
                }
                catch (System.Exception ex)
                {
                    Console.WriteLine(ex);
                }
                finally
                {
                    consumer.Close();
                    consumer.Dispose();
                }
            });
        }

        static async Task ProducerDemoAsync()
        {
            IProducer<string, string> producer = new ProducerBuilder<string, string>(new ProducerConfig
            {
                BootstrapServers = BootstrapServers,
                Acks = Acks.Leader,
                MessageSendMaxRetries = 5,
                BatchSize = 20,
                LingerMs = 3000,
            }).Build();

            try
            {
                while (true)
                {
                    DeliveryResult<string, string> deliveryResult = await producer.ProduceAsync(Topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = DateTime.Now.ToString() });
                    Console.WriteLine($"Producer::{deliveryResult.Key}::{deliveryResult.Value}::{deliveryResult.Partition.Value}::{deliveryResult.Offset.Value}::{Thread.CurrentThread.ManagedThreadId}");
                    // await Task.Delay(100);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);

            }
            finally
            {
                producer.Flush();
                producer.Dispose();
            }
        }
    }
}
