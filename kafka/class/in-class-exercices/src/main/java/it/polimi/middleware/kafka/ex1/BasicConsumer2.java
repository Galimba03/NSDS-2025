package it.polimi.middleware.kafka.ex1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BasicConsumer2 {
    private static final String defaultGroupId = "groupB";
    private static final String defaultTopic = "topicA";

    private static final String secondaryTopic = "topicB";
    private static final boolean waitAck = false;       // If it's needed to wait for an ACK message

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : defaultTopic;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Identifies the consumer group
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit)); // It commits automatically offsets
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs)); // How much does it do autocommit? 15s

        // Necessary
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Byte deserialize the key of the event got
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Byte deserialize the value of the event got

        // <Keys, Values> it consumes
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic)); // CountingAtomicConsumer subscribe to the topic

        props.clear();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Serialize the key of the message in bytes
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Serialize the value of the message in bytes

        // Instantiation of the kafka producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        while (true) {
            // Pulling of messages until they arrive. The time limit is 5 minutes. After that, it will stop
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

            for (final ConsumerRecord<String, String> record : records) {
                if(Character.isUpperCase(record.value().charAt(0))) {
                    String result = record.value().replaceAll("[A-Z]", "");
                    System.out.println(
                            "Topic: " + secondaryTopic +
                            "\tKey: " + record.key() +
                            "\tValue: " + result
                    );

                    final ProducerRecord<String, String> record_to_send = new ProducerRecord<>(secondaryTopic, record.key(), result);
                    final Future<RecordMetadata> future = producer.send(record_to_send);
                    if (waitAck) {
                        // It is waiting for a reply
                        try {
                            RecordMetadata ack = future.get();
                            System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                        } catch (InterruptedException | ExecutionException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }

        }
    }
}