package com.sias.kafka.producer2;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author Edgar
 * @create 2022-09-18 15:01
 * @faction:
 */

/*自定义分区*/
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
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

