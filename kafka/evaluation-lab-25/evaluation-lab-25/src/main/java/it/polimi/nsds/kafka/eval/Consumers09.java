package it.polimi.nsds.kafka.eval;

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

// Group number: 09
// Group members: Galimberti Matteo - Komisarjevsky Luca - Ghisolfi Davide

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
/*
    Yes, it's possible because the assignment is made based on the message key.
    The same key will always be on the same partition and each Merger will always read all messages with the same key.
    No key can be found in a different partition so "there's no loss of messages".
 */

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
/*
    Yes, the number of partition should be equal. This ensure that messages with the same
    key in both topics go to the same partition.
 */

// Is it possible to have more than one instance of Merger?
// If so, what is the relation between their group id?
/*
    Yes, but it depends on the group they belong to. They should have the same group_id so that
    load balancing is introduced. Messages are distributed between the instances of merger in the same group. This is only
    possible if and only if the group_id is the same.
 */

// Is it possible to have more than one partition for topic "merged"?
/*
    Yes, the sum has been already created in the Merger, so the Validator doesn't require ordering between different keys.
    Each key will always be assigned to the same partition.
 */

// Is it possible to have more than one instance of Validator?
// If so, what is the relation between their group id?
/*
    Yes, but they need to have the same group_id, so that messages are not duplicated.
*/

public class Consumers09 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];

        switch (stage) {
            case 0:
                System.err.println("Wrong stage");
            case 1:
                new Merger(serverAddr, groupId).execute();
                break;
            case 2:
                new Validator(serverAddr, groupId).execute();
                break;
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;

        // We disabled autoCommit in order to grant "at-least-once semantic"
        private final Boolean autoCommit = false;

        private static final List<String> INPUT_TOPICS = List.of("sensors1", "sensors2");
        private static final String outputTopic = "merged";

        public Merger(String serverAddr, String consumerGroupId) {
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

            // Disabling autocommit
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            // Read from the last committed offset
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(INPUT_TOPICS);

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            // Hash map for the last message read for each key coming from sensor 1
            final Map<String, Integer> lastMessageSensor1 = new HashMap<>();
            // Hash map for the last message read for each key coming from sensor 2
            final Map<String, Integer> lastMessageSensor2 = new HashMap<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println(
                            "Received key: " + record.key() +
                                    " | Value: " + record.value() +
                                    " | Topic: " + record.topic() +
                                    " | Partition: " + record.partition()
                    );

                    // Add to hash table based on the topic in input
                    switch (record.topic()) {
                        case "sensors1": {
                            lastMessageSensor1.put(record.key(), record.value());
                            break;
                        }
                        case "sensors2": {
                            lastMessageSensor2.put(record.key(), record.value());
                            break;
                        }
                    }

                    // Sum the last pair of messages with the record key
                    int sum = lastMessageSensor1.getOrDefault(record.key(), 0) + lastMessageSensor2.getOrDefault(record.key(), 0);
                    System.out.println(
                            "Sum is : " + sum
                    );

                    producer.send(new ProducerRecord<>(outputTopic, record.key(), sum));
                    producer.flush();
                }

                // At-least-once semantic commit
                consumer.commitSync();
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "merged";
        private static final List<String> OUTPUT_TOPICS = List.of("output1", "output2");

        private static final Boolean autoCommit = true;

        private static final String producerTransactionalId = "forwarderTransactionalId" + UUID.randomUUID();

        public Validator(String serverAddr, String consumerGroupId) {
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

            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if(records.isEmpty()) { continue; }

                for (final ConsumerRecord<String, Integer> record : records) {
                    try {
                        producer.beginTransaction();

                        System.out.println(
                                "Received key: " + record.key() +
                                        " | Value: " + record.value() +
                                        " | Topic: " + record.topic() +
                                        " | Partition: " + record.partition()
                        );

                        // For each output topic we send the received message.
                        for(String topic : OUTPUT_TOPICS) {
                            producer.send(new ProducerRecord<>(topic, record.key(), record.value()));
                        }
                        producer.flush();

                        producer.commitTransaction();
                    } catch (Exception e) {
                        producer.abortTransaction();
                    }
                }
            }
        }
    }
}