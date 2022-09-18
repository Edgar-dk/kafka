package com.sias.kafka.producer2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 15:03
 * @faction:
 */
/*自定义分区*/
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092 ");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.sias.kafka.producer2.MyPartitioner");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 50; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "sias " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata,
                                         Exception e) {
                    if (e == null) {
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}

