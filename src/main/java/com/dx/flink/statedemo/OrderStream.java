package com.dx.flink.statedemo;

import com.dx.flink.statedemo.pojo.Constants;
import com.dx.flink.statedemo.pojo.TableOne;
import com.dx.flink.statedemo.pojo.TableTwo;
import com.dx.flink.statedemo.sourcedemo.FileSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import static com.dx.flink.statedemo.pojo.TableOne.string2TableOne;
import static com.dx.flink.statedemo.pojo.TableTwo.string2TableTwo;

public class OrderStream {
    public static void main(String[] args) throws Exception {
        // 定义一个配置 import org.apache.flink.configuration.Configuration;包下
        Configuration configuration = new Configuration();

        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);

        // 执行环境使用当前配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         *  启用检查点机制
         *  ①设置检点点保存周期
         *  ②设置检查点模式 -
         *  ③设置检查点时间间隔
         *  ④设置检查点的最长时间
         *  ⑤设置同一时间只允许一个检查点
         *  ⑥一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的
         * Checkpoint【详细解释见备注】
         */
        env.enableCheckpointing(1000);
        env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.6.102:8020/test/"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);



        DataStreamSource<String> dataStreamSource1 = env.addSource(new FileSource(Constants.TABLE_ONE));
        DataStreamSource<String> dataStreamSource2 = env.addSource(new FileSource(Constants.TABLE_TWO));
        /**
         * 1、数据转换成为字符串(map)后并分流(keyBy)
         */
        KeyedStream<TableOne, Long> dataStream1 = dataStreamSource1.map(line -> string2TableOne(line)).keyBy(TableOne -> TableOne.getOrderId());
        KeyedStream<TableTwo, Long> dataStream2 = dataStreamSource2.map(line -> string2TableTwo(line)).keyBy(TableTwo -> TableTwo.getOrderId());

        /**
         * 2、数据关联后
         */
        dataStream1.connect(dataStream2)
                .flatMap(new EnrichmentFunction())
                .print();
        env.execute("OrderInfo......");
    }


    public static class EnrichmentFunction extends RichCoFlatMapFunction<TableOne,TableTwo, Tuple2<TableOne,TableTwo>>{
        // 定义第一个流的state
        private ValueState<TableOne> tableOneValueState;
        // 定义第二个流的state
        private ValueState<TableTwo> tableTwoValueState;

        public void open(Configuration parameters){
            // 注册状态
            tableOneValueState = getRuntimeContext().getState(new ValueStateDescriptor<TableOne>("info1",TableOne.class));
            tableTwoValueState = getRuntimeContext().getState(new ValueStateDescriptor<TableTwo>("info2",TableTwo.class));
        }


        @Override
        public void flatMap1(TableOne tableOne, Collector<Tuple2<TableOne, TableTwo>> collector) throws Exception {
            TableTwo value2 = tableTwoValueState.value();
            if(value2 != null){
                tableTwoValueState.clear();
                collector.collect(Tuple2.of(tableOne,value2));
            } else {
                tableOneValueState.update(tableOne);
            }
        }

        @Override
        public void flatMap2(TableTwo tableTwo, Collector<Tuple2<TableOne, TableTwo>> collector) throws Exception {
            TableOne value1 = tableOneValueState.value();
            if(value1 != null){
                tableOneValueState.clear();
                collector.collect(Tuple2.of(value1,tableTwo));
            } else {
                tableTwoValueState.update(tableTwo);
            }
        }
    }
}
