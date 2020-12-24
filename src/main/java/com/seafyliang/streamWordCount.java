/**
 * FileName: streamWordCount
 * Author:   SeafyLiang
 * Date:     2020/12/24 下午3:43
 * Description: 流处理 wordCount
 */
package com.seafyliang;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈流处理 wordCount〉
 *
 * @author SeafyLiang
 * @create 2020/12/24
 * @since 1.0.0
 */
public class streamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(8);

        // // 从文件中读取数据
        // String inputPath = "/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/hello.txt";
        // // DataStreamSource继承自DataStream 因此可直接转换成DataStream
        // DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用 parameter tool 工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // linux 命令行 nc -lk 7777
        // -l listen 监听
        // -k keep 保持

        // 用 parameter tool 工具从程序启动参数中提取配置项

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new batchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();

    }
}
