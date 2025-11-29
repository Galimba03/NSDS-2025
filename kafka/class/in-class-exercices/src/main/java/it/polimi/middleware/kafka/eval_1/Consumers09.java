package it.polimi.middleware.kafka.eval_1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number: 09
// Group members: blah blah blah

// Number of partitions for inputTopic (min, max): 1, n
// Number of partitions for outputTopic1 (min, max): 1, 1
// Number of partitions for outputTopic2 (min, max): 1, 1

// Number of instances of Consumer1 (and groupId of each instance) (min, max): 1, 1
// Number of instances of Consumer2 (and groupId of each instance) (min, max): 1, n

// Please, specify below any relation between the number of partitions for the topics
// and the number of instances of each Consumer

public class Consumers09 {

    // Args: 1/2 or A/B
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int consumerId = Integer.parseInt(args[0]);
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

        private static final boolean autoCommit = false;

        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic1";
        private static final String producerTransactionalId = "forwarderTransactionalId" + UUID.randomUUID();

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

            // The prof used a List of ConsumerRecords<String, Integer>
            int messageCount = 0;
            int sum = 0;
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if(!records.isEmpty()) {
                    for (final ConsumerRecord<String, Integer> record : records) {
                        sum += record.value();

                        if(messageCount >= 9) {
                            System.out.println(
                                    "Key: " + record.key() +
                                            " | Summed: " + sum +
                                            " | Partition: " + record.partition() +
                                            " | Offset: " + record.offset()
                            );
                            try {
                                producer.beginTransaction();
                                producer.send(new ProducerRecord<>(outputTopic, "sum", sum));

                                messageCount = 0;
                                sum = 0;

                                consumer.commitSync();
                                producer.commitTransaction();
                            } catch (Exception e) {
                                producer.abortTransaction();
                                e.printStackTrace();
                            }
                        }
                        messageCount++;
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
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            // You missed that!
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            Map<String, Integer> map = new HashMap<>();

            int messageCount = 0;

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if(!records.isEmpty()) {
                    for (final ConsumerRecord<String, Integer> record : records) {
                        map.put(record.key(), record.value() + map.getOrDefault(record.key(), 0));

                        if(messageCount >= 9) {
                            for(Map.Entry<String, Integer> entry : map.entrySet()) {
                                System.out.println(
                                    "Key: " + entry.getKey() +
                                            " | Sum: " + entry.getValue() +
                                            " | Partition: " + record.partition() +
                                            " | Offset: " + record.offset()
                                );

                                producer.send(new ProducerRecord<>(outputTopic, entry.getKey(), entry.getValue()));
                            }

                            messageCount = 0;
                            map.clear();
                        }

                        messageCount++;
                    }
                }
            }
        }
    }
}