package com.dx.flink.flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 优化2 : 抽离业务逻辑
 */
public class WordCount5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.6.102", 1111);
        SingleOutputStreamOperator<WordAndCount> wordCount = dataStreamSource.flatMap(new SplitWord())
                .keyBy("word")
                .sum("count");
        wordCount.print();
        env.execute("word count......");
    }


    /**
     * 业务逻辑抽离 : 单词分割
     */
    public static class SplitWord implements FlatMapFunction<String,WordAndCount>{

        @Override
        public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
            String[] fields = s.split(",");
            for (String word:fields) {
                collector.collect(new WordAndCount(word,1));
            }

        }
    }


    /**
     * 中间数据抽象为对象
     */
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
