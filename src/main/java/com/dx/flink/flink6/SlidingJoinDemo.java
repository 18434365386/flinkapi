package com.dx.flink.flink6;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SlidingJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //需要设置时间语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("192.168.6.102", 7777);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("192.168.6.102", 8888);
        DataStream<String> joinData = dataStreamSource1.join(dataStreamSource2)
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String input) throws Exception {
                        return input.split(",")[0];
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String input) throws Exception {
                        return input.split(",")[0];
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new JoinFunction<String, String, String>() {
                    @Override
                    public String join(String data1, String data2) throws Exception {
                        return data1 + "," + data2;
                    }
                });
        joinData.print("关联数据---");
        env.execute("job commit!!!");
    }
}

