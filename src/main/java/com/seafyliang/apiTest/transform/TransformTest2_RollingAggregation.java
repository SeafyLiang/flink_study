/**
 * FileName: TransformTest2_RollingAggregation
 * Author:   SeafyLiang
 * Date:     2021/1/8 上午9:47
 * Description: 滚动聚合
 */
package com.seafyliang.apiTest.transform;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈滚动聚合〉
 *
 * @author SeafyLiang
 * @create 2021/1/8
 * @since 1.0.0
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/sensor.txt");

        // 转换成SensorReading类型
        // DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
        //     @Override
        //     public SensorReading map(String s) throws Exception {
        //         String[] fields = s.split(",");
        //
        //         return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        //     }
        // });

        // 使用java8的lambda表达式 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组 按照sensor的id分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // // 返回类型可使用对应字段类型 而不是 Tuple
        // KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
        // // java8新特性：方法引用
        // KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(SensorReading::getId);


        // 滚动聚合，取当前最大的温度值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();

        env.execute();

    }
}
