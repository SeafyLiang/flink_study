/**
 * FileName: TransformTest6_unionStream
 * Author:   SeafyLiang
 * Date:     2021/1/20 下午3:48
 * Description: union多条流合并
 */
package com.seafyliang.apiTest.transform;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * 〈union多条流合并〉
 *
 * @author SeafyLiang
 * @create 2021/1/20
 * @since 1.0.0
 */
public class TransformTest6_unionStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/sensor.txt");


        // 使用java8的lambda表达式 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1. 分流，按照温度值30度为届分为两条流
        // split已过时，用side output代替
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        // highTempStream.print("high");
        // lowTempStream.print("low");
        // allTempStream.print("all");

        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading,Tuple2<String, Double>>(){
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);

        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), "normal");
            }
        });

        // resultStream.print();

        // 3. union联合多条流
        // 数据类型不同会报错
        // warningStream.union(lowTempStream);
        DataStream<SensorReading> unionStream = highTempStream.union(lowTempStream, allTempStream);

        unionStream.print();

        env.execute();

    }
}
