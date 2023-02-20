package com.dx.flink.flink5;

import com.dx.flink.flink5.pojo.VideoOrder;
import com.dx.flink.flink5.source.VideoOrderSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<VideoOrder> dataStreamSource = env.addSource(new VideoOrderSource());
        dataStreamSource.print("订单数据!!!");
        // 1、数据分组
        KeyedStream<VideoOrder, String> keyedStream = dataStreamSource.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder videoOrder) throws Exception {
                return videoOrder.getTitle();
            }
        });
        // 2、时间窗口聚合
        SingleOutputStreamOperator<VideoOrder> sinkData = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("money");
        sinkData.print();
        env.execute("5秒统计---");
    }
}
