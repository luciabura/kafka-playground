using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using Example.Proto;
using ProtobufHelpers;

namespace KafkaProducer.Protobuf
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", "10.200.0.1:29092" } };
            var topicName = "foo";

            using (var p = new Producer<Null, LogMsg>(config, null, new ProtoSerializer<LogMsg>()))
            {
                var cancelled = false;

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    string key = null;
                    string val = text;


                    // Calling .Result on the asynchronous produce request below causes it to
                    // block until it completes. Generally, you should avoid producing
                    // synchronously because this has a huge impact on throughput. For this
                    // interactive console example though, it's what we want.
                    var value = new LogMsg
                    {
                        IP = "127.0.0.1",
                        Message = val,
                        Severity = Severity.Info
                    };
                    // Calling .Result on the asynchronous produce request below causes it to
                    // block until it completes. Generally, you should avoid producing
                    // synchronously because this has a huge impact on throughput. For this
                    // interactive console example though, it's what we want.
                    var deliveryReport = p.ProduceAsync(topicName, null, value).Result;
                    Console.WriteLine(
                        deliveryReport.Error.Code == ErrorCode.NoError
                            ? $"delivered to: {deliveryReport.TopicPartitionOffset}"
                            : $"failed to deliver message: {deliveryReport.Error.Reason}"
                    );
                }
            }
        }
    }
}
