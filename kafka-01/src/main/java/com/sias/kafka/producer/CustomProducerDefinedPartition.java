package com.sias.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 14:57
 * @faction:
 */

/*自定义分区*/

public class CustomProducerDefinedPartition {
    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*1.自定义的分区，底层默认加载*/
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.sias.kafka.producer.MyPartitionZ");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i =0;i<50;i++){
            kafkaProducer.send(new ProducerRecord<>("first", "sias" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        System.out.println("主题："+recordMetadata.topic()+"分区："+recordMetadata.partition());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
