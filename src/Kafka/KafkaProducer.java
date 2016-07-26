package Kafka; /**
 * Created by tiger on 7/15/2016.
 */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer extends Thread {
    private static Producer<Integer, String> producer;
    private static final String topic= "mytopic";
    public static Map timeSent = null;

    public void initialize() {

        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", "localhost:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<Integer, String>(producerConfig);
    }
    public void publishMesssage() throws Exception{
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      /*  while (true){
            System.out.print("Enter message to send to kafka broker(Press 'Y' to close producer): ");
            String msg = null;
            msg = reader.readLine(); // Read message from console
            //Define topic name and message
            KeyedMessage<Integer, String> keyedMsg =
                    new KeyedMessage<Integer, String>(topic, msg);
            producer.send(keyedMsg); // This publishes message on given topic
            if("Y".equals(msg)){ break; }
            System.out.println("--> Message [" + msg + "] sent.Check message on Consumer's program console");
        }
*/
        KeyedMessage<Integer, String> keyedMsg =
                new KeyedMessage<Integer, String>(topic, "Hello Noor");

        System.out.println("Publish going to start");
        long start = System.currentTimeMillis();
        timeSent =  new LinkedHashMap();

        for(int i=1;i<=100000;i++) {
            producer.send(keyedMsg);
            timeSent.put(i, System.nanoTime());

        }
        System.out.println("Publish end");
        System.out.println("Time taken to publish :"+(((System.currentTimeMillis()-start) / 1000) % 60)+" seconds");

       // return;
    }

    public static void produce() throws Exception {
        KafkaProducer kafkaProducer = new KafkaProducer();
        // Initialize producer
        kafkaProducer.initialize();
        // Publish message
        kafkaProducer.publishMesssage();
        //Close the producer
        producer.close();
    }

    @Override
    public void run() {
        KafkaProducer kafkaProducer = new KafkaProducer();
        // Initialize producer
        kafkaProducer.initialize();
        // Publish message
        try {
            kafkaProducer.publishMesssage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //Close the producer
        producer.close();
    }
}