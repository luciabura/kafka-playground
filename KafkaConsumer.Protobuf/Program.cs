using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Example.Proto;
using ProtobufHelpers;

namespace KafkaConsumer.Protobuf
{
    class Program
    {
        static void Main(string[] args)
        {
            var cconfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", "localhost:9092" },
                { "group.id", Guid.NewGuid().ToString() }
            };
            var topicName = "foo";

            using (var consumer = new Consumer<Null, LogMsg>(cconfig, null, new ProtoDeserializer<LogMsg>()))
            {
                consumer.OnMessage += (_, msg)
                    => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                consumer.Subscribe(topicName);
                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;  // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}
