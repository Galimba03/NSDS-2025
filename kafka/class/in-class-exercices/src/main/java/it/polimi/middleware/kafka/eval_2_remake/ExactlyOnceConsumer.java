package it.polimi.middleware.kafka.eval_2_remake;

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

public class ExactlyOnceConsumer {
    private static final String serverAddr = "localhost:9092";
    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";
    private static final String groupId = "exactly-once-group";
    private static final String transactionalId = "exactly-once-transactional-id";

    public static void main(String[] args) {
        // Consumer Config
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // ✅ IMPORTANTE
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // Producer Config (CON TRANSACTIONAL ID)
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId); // ✅ UNICO per producer
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ✅ OBBLIGATORIO
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all"); // ✅ OBBLIGATORIO

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // ✅ INIZIALIZZA LE TRANSAZIONI (UNA VOLTA)
        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

            if (!records.isEmpty()) {
                try {
                    // 🔥 INIZIA TRANSAZIONE
                    producer.beginTransaction();

                    // PROCESSING E INVIO MESSAGGI
                    for (ConsumerRecord<String, String> record : records) {
                        String processedValue = processMessage(record.value());

                        // Invia messaggio processato
                        producer.send(new ProducerRecord<>(outputTopic, record.key(), processedValue));
                    }

                    // 🔥 COMMIT OFFSET NELLA TRANSAZIONE
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                    // 🔥 COMMIT TRANSAZIONE (ATOMICO)
                    producer.commitTransaction();

                    System.out.println("✅ Transaction committed - Processed " + records.count() + " messages");

                } catch (Exception e) {
                    System.err.println("❌ Transaction failed: " + e.getMessage());
                    // 🔥 ABORT TRANSAZIONE (rollback automatico)
                    producer.abortTransaction();
                }
            }
        }
    }

    private static String processMessage(String value) {
        // Elaborazione del messaggio
        return value.toUpperCase();
    }
}
