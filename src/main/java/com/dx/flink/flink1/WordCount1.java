package com.dx.flink.flink1;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  案例1: 统计单词个数
 */

public class WordCount1 {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 2、获取数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.6.102", 1111);
        // 3、处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(",");
                for (String word : fields) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordOne.keyBy(0).sum(1);
        // 4、数据输出
        wordCount.print();
        // 5、启动任务
        env.execute("job");
//         System.out.println(env.getExecutionPlan());
    }
}
