package it.polimi.middleware.kafka.eval_remake;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number:
// Group members:

// Number of partitions for inputTopic (min, max):
// Number of partitions for outputTopic1 (min, max):
// Number of partitions for outputTopic2 (min, max):

// Number of instances of Consumer1 (and groupId of each instance) (min, max):
// Number of instances of Consumer2 (and groupId of each instance) (min, max):

// Please, specify below any relation between the number of partitions for the topics
// and the number of instances of each Consumer

public class ConsumersXX {

    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int consumerId = Integer.valueOf(args[0]);
        String groupId = args[1];
        if (consumerId == 1) {
            Consumer1 consumer = new Consumer1(serverAddr, groupId);
            consumer.execute();
        } else if (consumerId == 2) {
            Consumer2 consumer = new Consumer2(serverAddr, groupId);
            consumer.execute();
        }
    }

    private static class Consumer1 {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic1";

        private static final String transactionId = "consumer1Id";

        public Consumer1(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            List<ConsumerRecord<String, Integer>> window = new ArrayList<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if (records.isEmpty()) {continue;}

                for (final ConsumerRecord<String, Integer> record : records) {

                    window.add(record);
                    if(window.size() >= 10) {
                        System.out.println("Window is full");

                        try {
                            producer.beginTransaction();

                            final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                            final Map<TopicPartition, Long> maxOffsets = new HashMap<>(); // I need to map the last offset for each parti
                            int sum = 0;
                            for (final ConsumerRecord<String, Integer> windowElement : window) {
                                sum += windowElement.value();

                                TopicPartition tp = new TopicPartition(windowElement.topic(), windowElement.partition());
                                long currentMax = maxOffsets.getOrDefault(tp, -1L);
                                if (windowElement.offset() > currentMax) {
                                    maxOffsets.put(tp, windowElement.offset());
                                }
                            }

                            for (Map.Entry<TopicPartition, Long> entry : maxOffsets.entrySet()) {
                                offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1));
                            }

                            System.out.println("Sum of values: " + sum);
                            System.out.println("Sending to output topic: " + outputTopic);

                            producer.send(new ProducerRecord<>(outputTopic, "sum", sum));
                            producer.sendOffsetsToTransaction(offsetMap, consumer.groupMetadata());

                            producer.commitTransaction();
                            System.out.println("✅ Transaction committed successfully");
                        } catch (Exception e) {
                            System.err.println("❌ Transaction failed: " + e.getMessage());
                            producer.abortTransaction();
                        }

                        window.clear();
                    }
                }
            }
        }
    }

    private static class Consumer2 {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic2";

        public Consumer2(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            Map<String, Integer> windowCounter = new HashMap<>();
            Map<String, Integer> windowSum = new HashMap<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    windowCounter.put(record.key(), windowCounter.getOrDefault(record.key(), 0) + 1);
                    windowSum.put(record.key(), windowSum.getOrDefault(record.key(), 0) + record.value());

                    if (windowCounter.get(record.key()) >= 10) {
                        System.out.println(
                                "Sending the window with" +
                                        "| Key: " + record.key() +
                                        "| Sum: " + windowSum.get(record.key())
                        );

                        producer.send(new ProducerRecord<>(outputTopic, record.key(), windowSum.get(record.key())));
                        windowCounter.remove(record.key());
                        windowSum.remove(record.key());
                    }
                }
            }
        }
    }
}