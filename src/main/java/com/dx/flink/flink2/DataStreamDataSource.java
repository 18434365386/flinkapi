package com.dx.flink.flink2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class DataStreamDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /** Source
         *  1、数据源之collection
         */
        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("hadoop");
        arr1.add("spark");
        arr1.add("flink");
        DataStreamSource<String> dataStreamSource = env.fromCollection(arr1);
        SingleOutputStreamOperator<String> sink = dataStreamSource.map(line -> {
            return "word+" + line;
        });
        sink.print();
        env.execute("source test......");
    }
}
