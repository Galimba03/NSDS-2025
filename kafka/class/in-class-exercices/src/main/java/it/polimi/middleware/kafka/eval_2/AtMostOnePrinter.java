package it.polimi.middleware.kafka.eval_2;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

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


public class AtMostOnePrinter {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        String groupId = args[0];

        Consumer consumer = new Consumer(serverAddr, groupId);
        consumer.execute();
    }

    private static class Consumer {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "inputTopic";

        private static final Integer threshold = 10;

        public Consumer(String serverAddr, String consumerGroupId) {
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

            // ✅ CONFIGURATION AT-MOST-ONCE
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto-commits
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Start from most recent messages

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if(records.isEmpty()) {
                    continue;
                }

                // 🔥 COMMIT OFFSETS BEFORE processing messages
                // This is the heart of the 'at-most-once' semantics: messages are lost but not duplicated
                consumer.commitSync();

                for(ConsumerRecord<String, Integer> record : records) {
                    processMessage(record);
                }
            }
        }

        private void processMessage(ConsumerRecord<String, Integer> record) {
            if(record.value() >= threshold) {
                System.out.println(
                        "Key: " + record.key() +
                                "| Value: " + record.value() +
                                "| Partition: " + record.partition() +
                                "| Offset: " + record.offset()
                );
            } else {
                System.out.println(
                        record.value() + " is not greater than " + threshold
                );
            }
        }
    }
}
