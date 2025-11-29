package it.polimi.middleware.kafka.ex1_solutiuon;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final String defaultConsumerGroupId = "groupA";
    private static final String defaultInputTopic = "topicA";

    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 5000;

    private static final String serverAddr = "localhost:9092";

    private static final String offsetResetStrategy = "latest";

    private static int readMsgs = 0;

    public  static void main(String[] args)
        throws Exception {
        // Inputting using the input arguments
        String consumerGroupId = args.length >= 1 ? args[0] : defaultConsumerGroupId;
        String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;

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

        while (readMsgs <= 100) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records) {
                System.out.println(
                    "Topic: " + record.topic() +
                    " Partition: " + record.partition() +
                    " Offset: " + record.offset()
                );
            }

            readMsgs++;
        }
    }
}
