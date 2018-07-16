using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Google.Protobuf;

namespace ProtobufHelpers
{
    public class ProtoDeserializer<T> : IDeserializer<T>
        where T : Google.Protobuf.IMessage<T>, new()
    {
        private MessageParser<T> _parser;

        public ProtoDeserializer() { _parser = new MessageParser<T>(() => new T()); }

        public IEnumerable<KeyValuePair<string, object>>
            Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;

        public void Dispose() { }

        public T Deserialize(string topic, byte[] data)
            => _parser.ParseFrom(data);
    }
}
