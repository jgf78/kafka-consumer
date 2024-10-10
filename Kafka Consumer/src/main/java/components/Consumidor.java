package components;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import model.Pedido;

public class Consumidor {
    private KafkaConsumer<String, Pedido> consumer;

    public Consumidor() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.143:9092");
        props.put("group.id", "grupoA");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); 
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Pedido.class.getName()); 

        consumer = new KafkaConsumer<>(props);
    }

    public void suscribir(String topic) {
        consumer.subscribe(List.of(topic));
        while (true) {
            ConsumerRecords<String, Pedido> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, Pedido> record : records) {
                System.out.println("***** EL REGISTRO ES *** " + record.value()); 
            }
        }
    }
}
