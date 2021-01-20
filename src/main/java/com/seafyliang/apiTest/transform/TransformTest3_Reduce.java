/**
 * FileName: TransformTest3_Reduce
 * Author:   SeafyLiang
 * Date:     2021/1/8 上午11:06
 * Description: Reduce规约(一般化)菊花
 */
package com.seafyliang.apiTest.transform;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈Reduce规约(一般化)聚合〉
 *
 * @author SeafyLiang
 * @create 2021/1/8
 * @since 1.0.0
 */
public class TransformTest3_Reduce {
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

        // 分组 按照sensor的id分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(),value2.getTemperature()));
            }
        });

        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce((curState, newData) -> {
            return new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature()));
        });

        resultStream.print();

        env.execute();

    }
}
