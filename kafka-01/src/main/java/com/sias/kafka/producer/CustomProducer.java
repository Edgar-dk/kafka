package com.sias.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 11:48
 * @faction:
 */

/*生产数据的普通方式*/

public class CustomProducer {
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
        *   发送的量是通过循环的形式去发送的
        *   注释的地方，是直接发送数据的，
        *   下面一个。是按照异步的形式发送数据的，
        *   把主要的信息返回给调用者，调用者，看到，已经完成了
        *   其实后续还在处理数据中，*/
        /*for (int i =0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","hello"+i));
        }*/
        for (int i =0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first", "hello" + i), new Callback() {
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
