package com.dx.flink.flink5;

import com.dx.flink.flink5.pojo.VideoOrder;
import com.dx.flink.flink5.source.VideoOrderSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SlidingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<VideoOrder> dataStreamSource = env.addSource(new VideoOrderSource());
        dataStreamSource.print("订单数据---");
        KeyedStream<VideoOrder, String> keyedStream = dataStreamSource.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder videoOrder) throws Exception {
                return videoOrder.getTitle();
            }
        });
        SingleOutputStreamOperator<VideoOrder> moneyData = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("money");
        moneyData.print("滑动窗口统计---");
        env.execute("滑动窗口......");
    }
}
