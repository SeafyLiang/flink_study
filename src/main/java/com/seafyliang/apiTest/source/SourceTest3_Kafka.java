/**
 * FileName: SourceTest3_Kafka
 * Author:   SeafyLiang
 * Date:     2021/1/5 下午3:39
 * Description: 从Kafka读取数据
 */
package com.seafyliang.apiTest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 〈从Kafka读取数据〉
 *
 * @author SeafyLiang
 * @create 2021/1/5
 * @since 1.0.0
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        // 从Kafka读取数据
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer010<String>("sensor",new SimpleStringSchema(),props));

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
