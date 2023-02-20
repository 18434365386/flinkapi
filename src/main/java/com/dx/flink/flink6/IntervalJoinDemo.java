package com.dx.flink.flink6;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("192.168.6.102", 7777);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("192.168.6.102", 8888);
        SingleOutputStreamOperator<String> joinData = dataStreamSource1.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String input) throws Exception {
                return input.split(",")[0];
            }
        }).intervalJoin(dataStreamSource2.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String input) throws Exception {
                return input.split(",")[0];
            }
        })).between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<String, String, String>() {
                    @Override
                    public void processElement(String data1, String data2, Context context, Collector<String> collector) throws Exception {
                        collector.collect(data1 + "," + data2);
                    }
                });
        joinData.print("关联数据--");
        env.execute("job join......");
    }
}
