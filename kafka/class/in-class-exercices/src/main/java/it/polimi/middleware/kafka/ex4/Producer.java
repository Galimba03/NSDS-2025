package it.polimi.middleware.kafka.ex4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    private static final String defaultTopic = "topicA";

    private static final int numMessages = 1000;
    private static final int waitBetweenMsgs = 1000;
    private static final boolean waitAck = false;

    private static final boolean idempotentProducer = true;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        final Random r = new Random();

        List<String> topics = args.length < 1 ?
                Collections.singletonList(defaultTopic) :
                Arrays.asList(args);

        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Important if you want to guarantee that the message was delivered one and only once time
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotentProducer));

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = "Key" + r.nextInt(1000);
            final String value = "Val" + i;

            System.out.println(
                "Topic: " + topic +
                "\tKey: " + key +
                "\tValue: " + value
            );

            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            final Future<RecordMetadata> future = producer.send(record);

            if(waitAck) {
                try {
                    RecordMetadata ack = future.get();
                    System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

}
