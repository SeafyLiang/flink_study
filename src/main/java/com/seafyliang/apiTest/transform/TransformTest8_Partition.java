/**
 * FileName: TransformTest8_Partition
 * Author:   SeafyLiang
 * Date:     2021/2/18 下午3:33
 * Description: 重新分区
 */
package com.seafyliang.apiTest.transform;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈重新分区〉
 *
 * @author SeafyLiang
 * @create 2021/2/18
 * @since 1.0.0
 */
public class TransformTest8_Partition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/sensor.txt");
        // 使用java8的lambda表达式 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("dataStream");

        // 1.shuffle 打乱分区
        DataStream<String> stringDataStream = inputStream.shuffle();
        // stringDataStream.print("stringDataStream");

        // 2.keyBy  相同key在同一个分区
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // keyedStream.print("keyedStream");

        // 3.global 全部放到下游第一个分区
        dataStream.global().print("global");

        env.execute();

    }
}
