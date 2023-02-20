package com.dx.flink.flink6;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class CoGroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("192.168.6.102", 7777);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("192.168.6.102", 8888);
        DataStream<Tuple2<String, String>> joinData = dataStreamSource1.coGroup(dataStreamSource2)
                .where(line -> line.split(",")[0])
                .equalTo(line -> line.split(",")[0])
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<String, String, Tuple2<String, String>>() {
                    @Override
                    public void coGroup(Iterable<String> input1, Iterable<String> input2, Collector<Tuple2<String, String>> collector) throws Exception {
                        for (String inputOne : input1) {
                            boolean flag = false;
                            for (String inputTwo : input2) {
                                collector.collect(new Tuple2<>(inputOne.split(",")[1], inputTwo.split(",")[1]));
                                flag = true;
                            }
                            if (!flag) {
                                collector.collect(new Tuple2<>(inputOne.split(",")[1], null));
                            }
                        }
                    }
                });
        joinData.print("关联数据--");
        env.execute("job join......");
    }
}
