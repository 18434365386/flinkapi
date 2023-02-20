package com.dx.flink.flink3;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

public class MapStateDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                                5L));
        // 输出：
        //(1,5.0)
        //(2,3.6666666666666665)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountWindowAverageWithMapState())
                .print();
        env.execute("TestStatefulApi");
    }
    public static class CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        // managed keyed state
        //1. MapState ：key 是一个唯一的值，value 是接收到的相同的 key 对应的 value 的值
        private MapState<String, Long> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册状态
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<String, Long>(
                            "average", // 状态的名字
                            String.class, Long.class); // 状态存储的数据类型
            mapState = getRuntimeContext().getMapState(descriptor);
        }
        @Override
        public void flatMap(Tuple2<Long, Long> element,
                            Collector<Tuple2<Long, Double>> out) throws Exception {
            mapState.put(UUID.randomUUID().toString(), element.f1);
            // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
            List<Long> allElements = Lists.newArrayList(mapState.values());
            if (allElements.size() >= 3) {
                long count = 0;
                long sum = 0;
                for (Long ele : allElements) {
                    count++;
                    sum += ele;
                }
                double avg = (double) sum / count;
                out.collect(Tuple2.of(element.f0, avg));
                // 清除状态
                mapState.clear();
            }
        }
    }
}
