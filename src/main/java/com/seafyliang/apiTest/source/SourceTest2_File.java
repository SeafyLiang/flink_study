/**
 * FileName: SourceTest2_File
 * Author:   SeafyLiang
 * Date:     2021/1/5 下午3:31
 * Description: 从文件读取数据
 */
package com.seafyliang.apiTest.source;

import com.seafyliang.apiTest.beans.Msg_log_line;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.sling.commons.json.JSONObject;

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
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("/Users/seafyliang/Downloads/日志/外网日志.txt");



        // 3. filter，筛选sensor_1开头的id对应的数据
        DataStream<String> filterStream = dataStream.filter(new FilterFunction<String>() {
            Integer num = 0;
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                // System.out.println(jsonObject.get("host"));
                if (jsonObject.has("host")){
                    // if (jsonObject.get("host").toString().equals("node119") && (jsonObject.get("msg_level").toString().equals("warn")|| jsonObject.get("msg_level").toString().equals("error"))){
                    String msg_level = jsonObject.get("msg_level").toString();
                    if (msg_level.equals("warn")|| msg_level.equals("error")){
                        num += 1;
                        System.out.println(num);
                        return true;
                    }
                }
                return false;

            }
        });
        // // 打印输出
        // // dataStream.print("dataStream");
        filterStream.print("filterStream");
        // 使用java8的lambda表达式 转换成SensorReading类型

        DataStream<Msg_log_line> outputStream = filterStream.map(line -> {


            JSONObject jsonObject = new JSONObject(line);
            System.out.println(jsonObject.get("message"));

            // String[] fields = line.split(",");
            return new Msg_log_line(jsonObject.get("message").toString());

        });
        //writeAsText 已过时，推荐使用StreamingFileSink
        String filePath = "/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/java/com/seafyliang/apiTest/sink/7-8warn_error_all.log";
        outputStream.writeAsText(filePath);

        env.execute();
    }
}
