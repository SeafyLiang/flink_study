/**
 * FileName: batchWordCount
 * Author:   SeafyLiang
 * Date:     2020/12/24 下午2:35
 * Description: 批处理wordcount
 */
package com.seafyliang;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 〈批处理wordcount〉
 *
 * @author SeafyLiang
 * @create 2020/12/24
 * @since 1.0.0
 */
public class batchWordCount {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "/Users/seafyliang/DEV/Code_projects/Java_projects/study_projects/flink_study/src/main/resources/hello.txt";
        // DataSource继承自DataSet 因此可直接转换成DataSet
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按 空格 分词展开，转换成(word,1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap( new MyFlatMapper() )
                .groupBy(0)     // 按照第一个位置的word分组
                .sum(1);          // 将第二个位置上的数据求和

        resultSet.print();
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            // 遍历所有word, 包成二元组输出
            for ( String word : words ){
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
