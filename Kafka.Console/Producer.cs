using Confluent.Kafka;

namespace Kafka.Console
{
    public class Producer : ApacheKafka
    {

        public Producer() 
            :base(){ }

        public void Produce(string msg=null) 
        {
            if (msg == null)
                msg = "Hello, This is Testing";

            var config = new ProducerConfig { BootstrapServers = _bootstrapServerAddress };
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            var message = new Message<Null, string> { Value = msg };
            
            producer.Produce(_topicName, message);    
            producer.Flush();
        }
    }
}
