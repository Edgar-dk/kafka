package com.sias.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 19:41
 * @faction:
 */

/*设置ACK的应答机制，以及，重复的次数（在失败的情况下）*/

public class CustomProducerACK {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        /*1.提高生产量的配置
        *   缓冲区的大小，每一批次的大小
        *   多少时间传输一次，压缩的方式*/
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        /*2.设置ACK的配置
        *   默认是-1，级别的，设置成1级别的
        *   传输的速率是非常快的
        *   并且发送的次数设置成三次（失败的情况下）*/
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i =0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","o"+i));
        }
        kafkaProducer.close();
    }
}
