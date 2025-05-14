package com.gjn.gd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Package com.gjn.dwd.dwd_brand_type
 * @Author jingnan.guo
 * @Date 2025/5/12 14:10
 * @description: 商品 品牌  品类级别    没有运用异步关联
 */
public class dwd_brand_type {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从kafka中读取 数据 创建动态表
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //kafkaStrDS.print("kafka>>>");

        //过滤出 一级品牌表  base_category1pDs
        SingleOutputStreamOperator<JSONObject> base_category1pDs = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("base_category1"));
       // base_category1pDs.print("1>>>");

        SingleOutputStreamOperator<JSONObject> base_category1DS = base_category1pDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                String base_category1Name = after.getString("name");
                object.put("id", id);
                object.put("base_category1Name", base_category1Name);
                return object;
            }
        });
        //base_category1DS.print("1DS>>>");

        //过滤出 二级品牌表  base_category2Ds
        SingleOutputStreamOperator<JSONObject> base_category2Ds = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("base_category2"));
        //base_category2Ds.print("2>>>");

        SingleOutputStreamOperator<JSONObject> base_category2DS = base_category2Ds.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                Integer category1Id = after.getInteger("category1_id");
                String category2Name = after.getString("name");
                object.put("id", id);
                object.put("category1Id", category1Id);
                object.put("category2Name", category2Name);
                return object;
            }
        });
        //base_category2DS.print("2DS>>>");

        //过滤出 三级品牌表  base_category3Ds
        SingleOutputStreamOperator<JSONObject> base_category3Ds = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("base_category3"));
        //base_category3Ds.print("3>>>");

        SingleOutputStreamOperator<JSONObject> base_category3DS = base_category3Ds.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                Integer category2Id = after.getInteger("category2_id");
                String category3Name = after.getString("name");
                object.put("id", id);
                object.put("category2Id", category2Id);
                object.put("category3Name", category3Name);
                return object;
            }
        });
        //base_category3DS.print("3DS>>>");

        //过滤出 商品表
        SingleOutputStreamOperator<JSONObject> sku_infoDs = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("sku_info"));
       // sku_infoDs.print("sku>>>");

        SingleOutputStreamOperator<JSONObject> sku_infoDS = sku_infoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                String price = after.getString("price");
                String skuName = after.getString("sku_name");
                String skuDesc = after.getString("sku_desc");
                String weight = after.getString("weight");
                String tmId = after.getString("tm_id");
                String category3Id = after.getString("category3_id");
                String createTime = after.getString("create_time");
                object.put("id", id);
                object.put("price", price);
                object.put("skuName", skuName);
                object.put("skuDesc", skuDesc);
                object.put("weight", weight);
                object.put("tmId", tmId);
                object.put("category3Id", category3Id);
                object.put("createTime", createTime);
                return object;
            }
        });
        //sku_infoDS.print("skuDS>>>");

        //过滤出 品牌表  base_trademark
        SingleOutputStreamOperator<JSONObject> base_trademarkDs = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("base_trademark"));
        //base_trademarkDs.print("base>>>");

        SingleOutputStreamOperator<JSONObject> base_trademarkDS = base_trademarkDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                String tmName = after.getString("tm_name");
                object.put("id", id);
                object.put("tmName", tmName);
                return object;
            }
        });
        //base_trademarkDS.print("baseDS>>>");


        // 将sku base trademark category3 category2 category1  进行关联
        SingleOutputStreamOperator<JSONObject> ds3 = sku_infoDS.keyBy(o -> o.getInteger("tmId"))
                .intervalJoin(base_trademarkDS.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //ds3.print();
        SingleOutputStreamOperator<JSONObject> ds4 = ds3.keyBy(o -> o.getInteger("category3Id"))
                .intervalJoin(base_category3DS.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //ds4.print();


        SingleOutputStreamOperator<JSONObject> ds5 = ds4.keyBy(o -> o.getInteger("category2Id"))
                .intervalJoin(base_category2DS.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //ds5.print();

        SingleOutputStreamOperator<JSONObject> ds6 = ds5.keyBy(o -> o.getInteger("category1Id"))
                .intervalJoin(base_category1DS.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //ds6.print();

        SingleOutputStreamOperator<JSONObject> DS7 = ds6.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject json) throws Exception {
                JSONObject result = new JSONObject();
                result.put("id", json.getInteger("id"));          // 提取 ID
                result.put("skuName", json.getString("skuName"));      // 提取名称
                result.put("price", json.getDouble("price"));    // 提取价格
                result.put("base_category1Name", json.getString("base_category1Name"));    // 提取价格
                result.put("category2Name", json.getString("category2Name"));    // 提取价格
                result.put("category3Name", json.getString("category3Name"));    // 提取价格
                result.put("createTime", json.getString("createTime"));    // 提取价格
                return result;
            }
        });

        DS7.print();

        env.execute();
    }
}