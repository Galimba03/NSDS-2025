package it.polimi.middleware.kafka.eval_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class AtMostOneProducer {
    private static final String defaultTopic = "inputTopic";
    private static final int numMessages = 1000;
    private static final int waitBetweenMsgs = 500;
    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
        List<String> topics = Collections.singletonList(defaultTopic);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

        // ✅ CONFIGURAZIONE AT-MOST-ONCE PER PRODUCER
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        props.put(ProducerConfig.ACKS_CONFIG, "1"); // o "0" per ancora più at-most-once
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        final KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        for (int i=0; i<numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = "Key" + r.nextInt(1000);
            final int value = r.nextInt(20);

            final ProducerRecord<String, Integer> record = new ProducerRecord<>(topic, key, value);

            producer.send(record);
            System.out.println("Sent: <" + key + ", " + value + ">");

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
