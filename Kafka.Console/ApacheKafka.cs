using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Console
{
    public class ApacheKafka
    {
        protected const string _bootstrapServerAddress = "localhost:9092";
        protected string _topicName;
        private int _numberOfPartitions = 1;
        private short _replicationFactor = 1;

        public ApacheKafka(
            string topicName = "default",
            int numberOfPartitions =1,
            short replicationFactor= 1
            )
        {
            if(!string.IsNullOrEmpty( topicName ) ) 
                _topicName = topicName;
            if(numberOfPartitions > 0)
                _numberOfPartitions = numberOfPartitions;
            if(replicationFactor > 0)
                _replicationFactor = replicationFactor;
        }

        public async Task AddTopicToKafka()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServerAddress,
            };

            using(var adminClient = new AdminClientBuilder(config).Build())
            {
                var topicSpecification = new TopicSpecification
                {
                    Name = _topicName,
                    NumPartitions = _numberOfPartitions,
                    ReplicationFactor = _replicationFactor
                };

                try {
                    adminClient.CreateTopicsAsync(new[] { topicSpecification}).Wait();
                    System.Console.WriteLine($"Topic '{_topicName}' created successfully.");
                }
                catch(Exception e)
                {
                    System.Console.WriteLine($"An error occurred while creating the topic: {e.Message}");
                }
            }

            return;
        }
    }
}
