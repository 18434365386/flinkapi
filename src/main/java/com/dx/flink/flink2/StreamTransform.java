package com.dx.flink.flink2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


/**
 * 业务需求: 统计来自两个数据流中带有had的字符串的个数。
 */
public class StreamTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 0、两个或多个数据流的联合，创建包含来自所有流的所有数据元的新流
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("192.168.6.102", 7777);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("192.168.144.67", 7777);
        DataStream<String> dataStreamSource = dataStreamSource1.union(dataStreamSource2);

        // 1、map - 将数据流中的每一个元素转换为另外一个元素( hadoop,spark --> test + hadoop,spark)
        SingleOutputStreamOperator<String> mapData = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("传输的值: " + value);
                return "test + " + value;
            }
        });
        mapData.print().setParallelism(1);

        // 2、filter - 过滤出来一些符合条件的元素，返回boolean值为true的元素 (test + hadoop,spark --> test + hadoop,spark)
        SingleOutputStreamOperator<String> filterData = mapData.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                String pattern = ".*had.*";
                return Pattern.matches(pattern, input);
            }
        });
        filterData.print().setParallelism(1);

        // 3、flatMap - 将数据流中的每一个元素转换为0...n个元素： (test + hadoop,spark --> (hadoop,1),(spark,1))
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapData = filterData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] data = input.split("\\+");
                String[] fields = data[1].split(",");
                for (String word : fields) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        flatMapData.print().setParallelism(1);

        // 4、KeyBy - 将流分区为不相交的分区，具有相同Keys的所有记录都分配给同一分区。 [(hadoop,1),(spark,1),(hadoop,1) --> (hadoop,2),(spark,1)]
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumData = flatMapData.keyBy(0).sum(1);
        sumData.print().setParallelism(1);

        env.execute("map + filter......");
    }
}
