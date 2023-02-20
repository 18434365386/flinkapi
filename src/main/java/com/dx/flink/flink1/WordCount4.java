package com.dx.flink.flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 优化三 : 统一使用 ParameterTool 进行传参
 */
public class WordCount4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Flink程序传入参数使用ParameterTool
         */
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> dataStreamSource = env.socketTextStream(host,port);
        SingleOutputStreamOperator<WordAndCount> wordCount = dataStreamSource.flatMap(new FlatMapFunction<String, WordAndCount>() {
            @Override
            public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
                String[] split = s.split(",");
                for (String word : split) {
                    collector.collect(new WordAndCount(word, 1));
                }
            }
        }).keyBy("word")
                .sum("count");

        wordCount.print();

        env.execute("word count......");
    }

    public static class WordAndCount{
        private String word;
        private int count;

        public WordAndCount() {
        }

        public WordAndCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
