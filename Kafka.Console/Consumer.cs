using Confluent.Kafka;

namespace Kafka.Console
{
    public class Consumer : ApacheKafka
    {
        protected string _groupId;
        public Consumer(string groupId = "default-group")
        : base()
        {
            _groupId = groupId;
        }

        public void Consume()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServerAddress,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_topicName);

            while (true)
            {
                try { 
                    var consumeResult = consumer.Consume();
                    if (consumeResult.IsPartitionEOF)
                    {
                        continue;
                    }

                    var message = consumeResult.Message;
                    System.Console.WriteLine($"Received message: {message.Value}");
                }catch (Exception e) 
                {}
            }
        }
    }
}
