/**
 * FileName: Transform7_RichFunction
 * Author:   SeafyLiang
 * Date:     2021/2/18 下午3:11
 * Description: 富函数
 */
package com.seafyliang.apiTest.transform;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈富函数〉
 *
 * @author SeafyLiang
 * @create 2021/2/18
 * @since 1.0.0
 */
public class TransformTest7_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/sensor.txt");


        // 使用java8的lambda表达式 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map( new MyMapper() );
        resultStream.print();
        env.execute();
    }

    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

    // 实现自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            // 获取上下文的各种信息
            // getRuntimeContext().getxxx
            // 获取上下文，当前子任务的序号
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是自定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}
