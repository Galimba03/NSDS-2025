package it.polimi.middleware.kafka.ex4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class CountingAtomicConsumer {
    private static final String defaultConsumerGroupId = "groupA";
    private static final String defaultInputTopic = "topicA";
    private static final String defaultOutputTopic = "topicB";
    private static final String defaultStateTopic = "consumer-state";

    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 5000;

    private static final String serverAddr = "localhost:9092";
    private static final String offsetResetStrategy = "latest";
    private static final String producerTransactionalId = "countingConsumerTransactionalId";

    // In-memory state: key -> message count
    private final Map<String, Integer> keyCountMap = new HashMap<>();

    public static void main(String[] args) {
        String consumerGroupId = args.length >= 1 ? args[0] : defaultConsumerGroupId;
        String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;
        String outputTopic = args.length >= 3 ? args[2] : defaultOutputTopic;
        String stateTopic = args.length >= 4 ? args[3] : defaultStateTopic;

        new CountingAtomicConsumer().run(consumerGroupId, inputTopic, outputTopic, stateTopic);
    }

    public void run(String consumerGroupId, String inputTopic, String outputTopic, String stateTopic) {
        // Consumer configuration
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // Producer configuration with transactions
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        // Load previous state (in a real system, this would be from persistent storage)
        loadInitialState();

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

            if (!records.isEmpty()) {
                producer.beginTransaction();

                try {
                    // Process each record and update counts
                    for (final ConsumerRecord<String, String> record : records) {
                        final String key = record.key();
                        final String value = record.value();

                        // Update in-memory count
                        int currentCount = keyCountMap.getOrDefault(key, 0);
                        keyCountMap.put(key, currentCount + 1);

                        // Create output message with count information
                        String outputValue = String.format("%s (total count: %d)", value.toLowerCase(), currentCount + 1);

                        System.out.println(
                                "Key: " + key +
                                        " | Value: " + value +
                                        " | Total Count: " + (currentCount + 1) +
                                        " | Partition: " + record.partition() +
                                        " | Offset: " + record.offset()
                        );

                        // Forward to output topic
                        producer.send(new ProducerRecord<>(outputTopic, key, outputValue));

                        // Store state update (in a real system, this would be batched)
                        producer.send(new ProducerRecord<>(stateTopic, key, String.valueOf(currentCount + 1)));
                    }

                    // Commit offsets within transaction
                    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsetMap.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsetMap, consumer.groupMetadata());
                    producer.commitTransaction();

                    System.out.println("Transaction committed successfully. Processed " + records.count() + " messages.");

                } catch (Exception e) {
                    System.err.println("Error processing transaction: " + e.getMessage());
                    producer.abortTransaction();
                    // In a real system, we might want to seek back to the last committed offset
                    // consumer.seekToBeginning(consumer.assignment());
                }
            }
        }
    }

    private void loadInitialState() {
        // In a production system, this would load state from:
        // - Kafka state topic (reading from the beginning)
        // - External database
        // - Local persistent storage

        // For this example, we start with empty state
        // A real implementation would recover state from the stateTopic
        System.out.println("Initializing with empty state");
        keyCountMap.clear();
    }

    // Helper method to get current counts (for monitoring/debugging)
    public Map<String, Integer> getCurrentCounts() {
        return new HashMap<>(keyCountMap);
    }
}