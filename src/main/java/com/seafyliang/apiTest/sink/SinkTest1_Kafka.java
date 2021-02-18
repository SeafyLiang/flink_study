/**
 * FileName: SinkTest1_Kafka
 * Author:   SeafyLiang
 * Date:     2021/2/18 下午3:45
 * Description: sink_kafka
 */
package com.seafyliang.apiTest.sink;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * 〈sink_kafka 案例-消息管道〉
 * flink消费 sensor主题的消息，推送到sinktest主题
 *
 * 生产者：
 * bash-4.4# ./kafka-console-producer.sh --broker-list 192.168.10.35:9092 --topic sensor
 * >sensor_1,1547718199,35.8
 * 消费者：
 * bash-4.4# ./kafka-console-consumer.sh --bootstrap-server 192.168.10.35:9092 --topic sinktest  --from-beginning
 * SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
 *
 * @author SeafyLiang
 * @create 2021/2/18
 * @since 1.0.0
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.10.35:9092");

        // 从Kafka读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer010<String>("sensor",new SimpleStringSchema(),props));
        // 使用java8的lambda表达式 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink( new FlinkKafkaProducer010<String>("192.168.10.35:9092", "sinktest", new SimpleStringSchema()));


        env.execute();
    }
}
