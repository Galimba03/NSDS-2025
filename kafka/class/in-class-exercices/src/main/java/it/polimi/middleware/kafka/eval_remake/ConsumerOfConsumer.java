package it.polimi.middleware.kafka.eval_remake;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class ConsumerOfConsumer {

    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        String groupId = args[0];

        Consumer consumer = new Consumer(serverAddr, groupId);
        consumer.execute();
    }

    private static class Consumer {

        private final String serverAddr;
        private final String groupId;

        private static final String inputTopic = "outputTopic1";


        public Consumer(String serverAddr, String groupId) {
            this.serverAddr = serverAddr;
            this.groupId = groupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            while(true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                if (records.isEmpty()) {continue;}

                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println(
                            "Key: " + record.key() +
                                    "| Value: " + record.value()
                    );
                }
            }
        }
    }

}
