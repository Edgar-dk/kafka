package com.sias.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 11:48
 * @faction:
 */
public class CustomProducerPartition {
    public static void main(String[] args) {
        /*1.创建这个类目的是为了读取配置文件*/
        Properties properties = new Properties();
        /*2.kafka连接对应的集群
        *   里面的参数，是producer的，
        *   可以去指定连接那个服务器*/
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        /*3.走到序列化，要指定key,value的类型
        *   在前面写上key的话，在后面，在写上key的值是多少
        *   同理，value也是这样的*/
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*4.创建kafka对象,
        *   配置文件，需要在创建kafka对象之前，*/
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        /*5.发送数据
        *   partition,先按照分区去发送数据，没有分区的话，在按照key发送
        *   ，按照key发送的话，取模运算，不指定的话，按照粘性发送，第一次先送
        *   到一个随机的分区，然后，数据一直发送这个分区里面，数据满的话，在随机
        *   更换一个分区*/
        for (int i =0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first",0,"", "hello" + i) ,new Callback() {
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
