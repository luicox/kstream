package com.luico.kstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KStream0StandinFlagController {

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "standin-processor-app");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-56d1g.eastus.azure.confluent.cloud:9092"); // Reemplaza con tu endpoint
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-921jm.us-east-2.aws.confluent.cloud:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
// Configuración de seguridad para Confluent Cloud
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username='NB3FSC6TCWWRAY37' " +
                        "password='z8FPnGI3ZuWdqNYHhS65uGdL6WpiRDS9pNXMsj+H5CTOoq9DBQMi88TV8a85yNBd';");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Activar standin
            producer.send(new ProducerRecord<>("standin_topico_zero_tecnico", "standin_flag_key", "INACTIVE"));

            // O para desactivar:
            // producer.send(new ProducerRecord<>("flag-standin", "standin_flag", "INACTIVE"));

            producer.flush();
            System.out.println("Estado de standin actualizado");
        }
    }
}
