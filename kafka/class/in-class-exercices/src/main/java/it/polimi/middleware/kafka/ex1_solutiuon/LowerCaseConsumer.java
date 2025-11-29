package it.polimi.middleware.kafka.ex1_solutiuon;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class LowerCaseConsumer {
    /*
        WHAT HAVE I LEARNED
        - Different group, same topic don't create problems. What does are same group, same topic.
        - If same group and same topic and only one partition => only one consumer per group can work at a time,
            If 2 are activated, 1 works, the other one not, until the first crash and kafka re-organize containers.
     */
    private static final String defaultConsumerGroupId = "groupB";
    private static final String defaultInputTopic = "topicA";
    private static final String defaultOutputTopic = "topicB";

    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 5000;

    private static final String serverAddr = "localhost:9092";

    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // Inputting using the input arguments
        String consumerGroupId = args.length >= 1 ? args[0] : defaultConsumerGroupId;
        String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;
        String outputTopic = args.length >= 3 ? args[2] : defaultOutputTopic;

        // CountingAtomicConsumer
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy); // latest strategy

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // Producer
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records) {
                final String key = record.key();
                final String newValue = record.value().toLowerCase();

                System.out.println(
                        "Topic: " + record.topic() +
                        " Partition: " + record.partition() +
                        " Offset: " + record.offset()
                );

                producer.send(new ProducerRecord<>(outputTopic, key, newValue));
            }
        }
    }
}
