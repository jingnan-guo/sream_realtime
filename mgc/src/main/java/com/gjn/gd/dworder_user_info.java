package com.gjn.gd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.gjn.dwd.dworder_user_info
 * @Author jingnan.guo
 * @Date 2025/5/14 20:06
 * @description: 订单宽表  和   日志表  结合
 */
public class dworder_user_info {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_userInfo")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> user = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //user.print("user->>");



        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_order")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> order = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //order.print("order->>");

        //userJSON->>> {"device_35_39":0.04,"os":"iOS","device_50":0.02,"search_25_29":0,"ch":"Appstore","pv":6,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"113","device_25_29":0.06,"md":"iPhone 14","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone","search_30_34":0}
        SingleOutputStreamOperator<JSONObject> userJSON = user.map(JSONObject::parseObject);
//        userJSON.print("userJSON->>");

        //orderJSON->>> {"birthday":"1993-08-09","decade":1990,"category2_name":"大 家 电","gender":"M","orderId":1500,"zodiac_sign":"狮子座","create_ts":1747083896000,"tm_name":"TCL","uid":69,"unit_height":"cm","category1_name":"家用电器","orderPrice":11999.0,"phone_num":"13692195295","id":69,"skuId":19,"email":"n6npyp6@gmail.com","category2_id":"16","height":"183","create_time":1746833326000,"weight":"59","category1_id":"3","login_name":"n6npyp6","tm_id":"4","total_amount":59692.2,"user_id":69,"name":"呼延江超","user_level":"1","unit_weight":"kg","category3_name":"平板电视","category3_id":"86","ts_ms":1747055497144,"age":31}
        SingleOutputStreamOperator<JSONObject> orderJSON = order.map(JSONObject::parseObject);
//        orderJSON.print("orderJSON->>");

        SingleOutputStreamOperator<JSONObject> orderDS = orderJSON.keyBy(o -> o.getString("user_id"))
                .intervalJoin(userJSON.keyBy(o -> o.getString("uid")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        orderDS.print("DS-->");

        env.execute();
    }
}
