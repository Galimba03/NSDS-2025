package it.polimi.middleware.kafka.eval_2_remake;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class AtLeastOnceConsumer {
    private static final String serverAddr = "localhost:9092";
    private static final String topic = "inputTopic";
    private static final String groupId = "at-least-once-group";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // ✅ CONFIGURAZIONE AT-LEAST-ONCE
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disabilita auto-commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Leggi tutti i messaggi

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

            for (ConsumerRecord<String, String> record : records) {
                // 🔥 PROCESSING PRIMA del commit
                // Se crash qui, il messaggio verrà riprocessato (duplicato)
                processMessage(record);
            }

            // 🔥 COMMIT OFFSET DOPO il processing
            // Questo è il cuore di at-least-once
            consumer.commitSync();
        }
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        System.out.println(
                "Processing - Key: " + record.key() +
                        ", Value: " + record.value() +
                        ", Offset: " + record.offset()
        );
        // Simula processing che potrebbe fallire
        if (Math.random() < 0.1) { // 10% chance di crash
            throw new RuntimeException("Simulated crash during processing!");
        }
    }
}
