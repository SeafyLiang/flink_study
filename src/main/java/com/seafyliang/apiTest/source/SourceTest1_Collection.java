/**
 * FileName: SourceTest1_Collection
 * Author:   SeafyLiang
 * Date:     2021/1/5 下午3:13
 * Description:
 */
package com.seafyliang.apiTest.source;

import com.seafyliang.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 〈从集合、元素读取数据〉
 *
 * @author SeafyLiang
 * @create 2021/1/5
 * @since 1.0.0
 */
// 定义样例类，传感器_id，时间戳，温度
public class SourceTest1_Collection{
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，使数据顺序一致
        env.setParallelism(1);

        // 1.Source: 从集合读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 66, 888);

        // 2.打印输出
        dataStream.print("data");
        integerDataStream.print("int");

        // 3. 执行任务
        env.execute();

    }
}
