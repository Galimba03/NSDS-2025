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


public class PopularTopicConsumer {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        String groupId = args[0];

        Consumer consumer = new Consumer(serverAddr, groupId);
        consumer.execute();
    }

    private static class Consumer {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final List<String> TOPICS_TO_MONITOR =
                List.of("inputTopic1", "inputTopic2", "inputTopic3");
        private static final String autoCommit = "true";
        private static final Integer autoCommitIntervalMs = 1000;

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

            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Start from most recent messages
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(TOPICS_TO_MONITOR);

            Map<String, Integer> topicsCount = new HashMap<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if(records.isEmpty()) {
                    continue;
                }

                for(ConsumerRecord<String, Integer> record : records) {
                    processMessage(record, topicsCount);
                }
            }

        }

        private void processMessage(ConsumerRecord<String, Integer> record, Map<String, Integer> topicsCount) {
            if(!topicsCount.containsKey(record.topic())) {
                topicsCount.put(record.topic(), 1);
            } else {
                topicsCount.put(record.topic(), topicsCount.get(record.topic()) + 1);
            }

            String keyWithMaxValue = topicsCount.entrySet()
                    .stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);

            if(keyWithMaxValue != null) {
                System.out.println(
                        "Most famous topic is: " +  keyWithMaxValue +
                                " with the value of " +  topicsCount.get(keyWithMaxValue)
                );
            }
        }
    }
}
