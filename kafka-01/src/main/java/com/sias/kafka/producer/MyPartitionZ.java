package com.sias.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author Edgar
 * @create 2022-09-18 11:48
 * @faction:
 */

/*自定义分区需要联合CustomProducerDefinedPartition使用*/

public class MyPartitionZ implements Partitioner{

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*1.这个地方容易出错，是topic地方，换成其他的字母是不行的*/
        // 获取消息
        String msgValue = value.toString();
        // 创建 partition
        int partition;
        // 判断消息是否包含 atguigu
        if (msgValue.contains("atguigu")) {
            partition = 0;
        } else {
            partition = 1;
        }
        // 返回分区号
        return partition;
    }

    // 关闭资源
    @Override
    public void close() {
    }

    // 配置方法
    @Override
    public void configure(Map<String, ?> configs) {
    }
}
