import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SynchronousProducer {

    public static void main(String[] args) throws Exception {

        //Assign topicName to string variable
        String topicName = "quickstart-events1";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        setupProps(props);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                // we can specify here partition, key value or just value
                // most minimal is only topic and partition
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 1, Integer.toString(i), Integer.toString(i));
                try {
                    // message will be sent to the buffer and will be sent to the broker in separate thread.
                    // send() returns Future object with RecordMetadata
                    // here we are using Future get() method to wait for a reply from Kafka.
                    // if there will be no errors RecordMetadata will be retrieved so we can extract
                    // offset the message was written to and other metadata.

                    producer.send(record).get();
                } catch (Exception e) {
                    // KafkaProducer has two types of errors.
                    // Retriable errors --> can be resolved by sending the message again.
                    // i.e. Connection error, not leader for partition
                    // KafkaProducer can be configured to automatically retry and send
                    // info about the exceptions only when limit was exhausted

                    // Message size too large for example will not be retry

                    e.printStackTrace();
                }

            }
            System.out.println("Message sent successfully");
        }
    }

    private static void setupProps(Properties props) {
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }
}