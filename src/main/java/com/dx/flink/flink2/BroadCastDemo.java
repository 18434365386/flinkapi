package com.dx.flink.flink2;


import it.unimi.dsi.fastutil.Hash;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * broadcast广播变量
 * 需求：
 * flink会从数据源中获取到用户的姓名
 * 最终需要把用户的姓名和年龄信息打印出来
 * 分析：
 * 所以就需要在中间的map处理的时候获取用户的年龄信息
 * 建议把用户的关系数据集使用广播变量进行处理
 *
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1、准备广播数据
        ArrayList<Tuple2<String,Integer>> arr = new ArrayList<>();
        arr.add(new Tuple2<>("lizhi",18));
        arr.add(new Tuple2<>("wangwu",21));
        arr.add(new Tuple2<>("zhangsan",20));

        DataSource<Tuple2<String, Integer>> tuple2Data = env.fromCollection(arr);

        /**
         *  2、处理需要广播的数据,将数据集转换为map类型
         *      map中的key: 用户姓名
         *      value: 就是年龄。
         */

        DataSet<HashMap<String, Integer>> toBroadcast = tuple2Data.map(new MapFunction<Tuple2<String, Integer>, HashMap<String,Integer>>() {

            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> valueTuple2) throws Exception {
                HashMap<String, Integer> hashMap = new HashMap<>();
                hashMap.put(valueTuple2.f0,valueTuple2.f1);
                return hashMap;
            }
        });


        // 3、初始化数据
        DataSource<String> DataSource = env.fromElements("lizhi", "wangwu", "zhangsan");


        /**
         *  广播数据: withBroadcastSet
         *  获取广播数据:
         */
        MapOperator<String, String> result = DataSource.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");

        result.print();
    }
}
