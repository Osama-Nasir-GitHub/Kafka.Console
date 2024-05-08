using Kafka.Console;

Console.WriteLine("********* Execution Started **********");

var producer = new Producer();
producer.Produce();

var consumer = new Consumer();
consumer.Consume();