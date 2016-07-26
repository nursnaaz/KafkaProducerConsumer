package Kafka; /**
 * Created by tiger on 7/15/2016.
 */
   import java.util.*;
           import kafka.consumer.Consumer;
           import kafka.consumer.ConsumerConfig;
           import kafka.consumer.ConsumerIterator;
           import kafka.consumer.KafkaStream;
           import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {
    public static Map timeRecieved = null;

    private ConsumerConnector consumerConnector = null;
    private final String topic = "mytopic";

    public void initialize() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "testgroup");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "300");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig conConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() {
        //Key = topic name, Value = No. of threads for topic
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, new Integer(1));

        //ConsumerConnector creates the message stream for each topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);
        timeRecieved =  new LinkedHashMap();
        System.out.println("3");

        // Get Kafka stream for topic 'mytopic'
        List<KafkaStream<byte[], byte[]>> kStreamList =
                consumerStreams.get(topic);
        // Iterate stream using ConsumerIterator
        int i=0;
        for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();

            while (consumerIte.hasNext()) {
                i++;
                System.out.println(i+" Message consumed from topic [" + topic + "] : " +
                        new String(consumerIte.next().message()));
                timeRecieved.put(i,System.nanoTime());

            }
            System.out.println("4");

        }
        System.out.println("5");

        System.exit(0);
        //Shutdown the consumer connector
        if (consumerConnector != null)  {
            consumerConnector.shutdown();
            System.out.println("5");

        }

    }

    public static void subscribe() throws InterruptedException {
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        // Configure Kafka consumer
        kafkaConsumer.initialize();
        // Start consumption
        kafkaConsumer.consume();
    }

    @Override
    public void run() {

        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        System.out.println("1");
        // Configure Kafka consumer
        kafkaConsumer.initialize();
        System.out.println("2");
        // Start consumption
        kafkaConsumer.consume();
        System.out.println("6");
        notifyAll();
    }
}

