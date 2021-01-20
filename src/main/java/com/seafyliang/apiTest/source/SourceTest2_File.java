/**
 * FileName: SourceTest2_File
 * Author:   SeafyLiang
 * Date:     2021/1/5 下午3:31
 * Description: 从文件读取数据
 */
package com.seafyliang.apiTest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 〈从文件读取数据〉
 *
 * @author SeafyLiang
 * @create 2021/1/5
 * @since 1.0.0
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/sensor.txt");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
