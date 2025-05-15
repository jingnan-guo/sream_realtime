package com.gjn.gd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.func.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Date;


/**
 * @Package com.gjn.dwd.dwdAgeThemeSummaryDetails
 * @Author jingnan.guo
 * @Date 2025/5/14 10:21
 * @description: 年龄主题汇总   异步关联
 */
public class dwdAgeThemeSummaryDetails  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从kafka中读取 数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //kafkaStrDS.print("kafka>>>");

        //过滤出  订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("order_info"));
        //order_info.print("order_info>>>");

        //过滤出 订单详情表
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("order_detail"));
        //order_detail.print("order_detailDs>>>");


        //提取 订单表的字段
        //order_infoDS>>>> {"create_time":"2025-05-10 04:42:08","total_amount":4397.0,"user_id":83,"id":1446}
        SingleOutputStreamOperator<JSONObject> order_infoDS = order_info.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                Double totalAmount = after.getDouble("total_amount");
                String userId = after.getString("user_id");
                Date createTime = after.getDate("create_time");
                object.put("id",id);
                object.put("total_amount",totalAmount);
                object.put("user_id",userId);
                object.put("create_time",createTime);
                return object;
            }
        });
        //order_infoDS.print("order_infoDS>>>");

        //提取 订单详情表 字段
        //order_detailDS>>>> {"orderId":1338,"orderPrice":3299.0,"id":2159,"skuId":21}
        SingleOutputStreamOperator<JSONObject> order_detailDS = order_detail.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                Integer orderId = after.getInteger("order_id");
                Integer skuId = after.getInteger("sku_id");
                Double orderPrice = after.getDouble("order_price");
                object.put("id",id);
                object.put("orderId",orderId);
                object.put("skuId",skuId);
                object.put("orderPrice",orderPrice);
                return object;
            }
        });
        //order_detailDS.print("order_detailDS>>>");

        // 关联 订单表  和  订单详情表
        //大表>>>> {"create_time":"2025-05-10 06:08:03","total_amount":40895.1,"user_id":69,"orderId":1470,"orderPrice":6499.0,"id":2434,"skuId":3}
        SingleOutputStreamOperator<JSONObject> orderInfoDS = order_infoDS.keyBy(o -> o.getInteger("id"))
                .intervalJoin(order_detailDS.keyBy(o -> o.getInteger("orderId")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });

        //orderInfoDS.print("大表>>>");

        //关联 sku 取 tm_id
        SingleOutputStreamOperator<JSONObject> orderDS2 = orderInfoDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = com.gjn.func.HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        com.gjn.func.HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        String skuId = jsonObject.getString("skuId");
                        JSONObject skuInfoJsonObj = com.gjn.func.HBaseUtil.getRow(hbaseConn, "ns_jingnan_guo", "dim_sku_info", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("tm_id", skuInfoJsonObj.getString("tm_id"));
                        a.put("category3_id", skuInfoJsonObj.getString("category3_id"));

                        return a;
                    }
                }
        );
        //orderDS2.print("ds2-->");
        //关联 品牌
        //{"birthday":"1977-08-09","decade":1970,"gender":"F","orderId":1390,"zodiac_sign":"狮子座","create_ts":1747083896000,"tm_name":"索芙特","uid":14,"unit_height":"cm","orderPrice":129.0,"phone_num":"13425678298","id":14,"skuId":26,"email":"xk2xoa2d7b@googlemail.com","height":"148","create_time":"2025-05-10 00:44:52","weight":"41","login_name":"xk2xoa2d7b","tm_id":"8","total_amount":40219.0,"user_id":14,"name":"孟倩婷","user_level":"2","unit_weight":"kg","category3_id":"477","ts_ms":1747055497142,"age":47}
        SingleOutputStreamOperator<JSONObject> orderDS3 = orderDS2.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = com.gjn.func.HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        com.gjn.func.HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        String tm_id = jsonObject.getString("tm_id");
                        JSONObject skuInfoJsonObj = com.gjn.func.HBaseUtil.getRow(hbaseConn, "ns_jingnan_guo", "dim_base_trademark", tm_id, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("tm_name", skuInfoJsonObj.getString("tm_name"));

                        return a;
                    }
                }
        );
        //orderDS3.print();



        //关联 三级品类
        //DS4-->> {"birthday":"1993-08-09","decade":1990,"gender":"M","orderId":1500,"zodiac_sign":"狮子座","create_ts":1747083896000,"tm_name":"Redmi","uid":69,"unit_height":"cm","orderPrice":999.0,"phone_num":"13692195295","id":69,"skuId":4,"email":"n6npyp6@gmail.com","category2_id":"13","height":"183","create_time":"2025-05-10 07:28:46","weight":"59","login_name":"n6npyp6","tm_id":"1","total_amount":59692.2,"user_id":69,"name":"呼延江超","user_level":"1","unit_weight":"kg","category3_name":"手机","category3_id":"61","ts_ms":1747055497144,"age":31}
        SingleOutputStreamOperator<JSONObject> orderDS4 = orderDS3.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = com.gjn.func.HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        com.gjn.func.HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        String skuId = jsonObject.getString("category3_id");
                        JSONObject skuInfoJsonObj = com.gjn.func.HBaseUtil.getRow(hbaseConn, "ns_jingnan_guo", "dim_base_category3", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category3_name", skuInfoJsonObj.getString("name"));
                        a.put("category2_id", skuInfoJsonObj.getString("category2_id"));

                        return a;
                    }
                }
        );
        //orderDS4.print("DS4-->");

        //关联 二级品类
        //ds5-->> {"birthday":"2004-05-09","decade":2000,"category2_name":"手机通讯","orderId":1499,"zodiac_sign":"金牛座","create_ts":1747083896000,"tm_name":"苹果","uid":13,"unit_height":"cm","orderPrice":8197.0,"phone_num":"13438715447","id":13,"skuId":9,"email":"46qnx87866d@163.com","category2_id":"13","height":"189","create_time":"2025-05-10 07:30:40","weight":"39","category1_id":"2","login_name":"46qnx87866d","tm_id":"2","total_amount":21133.0,"user_id":13,"name":"闻人松善","user_level":"1","unit_weight":"kg","category3_name":"手机","category3_id":"61","ts_ms":1747055497142,"age":21}
        SingleOutputStreamOperator<JSONObject> DS5 = orderDS4.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = com.gjn.func.HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        com.gjn.func.HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        String skuId = jsonObject.getString("category2_id");
                        JSONObject skuInfoJsonObj = com.gjn.func.HBaseUtil.getRow(hbaseConn, "ns_jingnan_guo", "dim_base_category2", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category2_name", skuInfoJsonObj.getString("name"));
                        a.put("category1_id", skuInfoJsonObj.getString("category1_id"));

                        return a;
                    }
                }
        );
        //DS5.print("ds5-->");

        //关联 一级品类
        //ds6-->> {"birthday":"1993-08-09","decade":1990,"category2_name":"大 家 电","gender":"M","orderId":1500,"zodiac_sign":"狮子座","create_ts":1747083896000,"tm_name":"TCL","uid":69,"unit_height":"cm","category1_name":"家用电器","orderPrice":6699.0,"phone_num":"13692195295","id":69,"skuId":17,"email":"n6npyp6@gmail.com","category2_id":"16","height":"183","create_time":"2025-05-10 07:28:46","weight":"59","category1_id":"3","login_name":"n6npyp6","tm_id":"4","total_amount":59692.2,"user_id":69,"name":"呼延江超","user_level":"1","unit_weight":"kg","category3_name":"平板电视","category3_id":"86","ts_ms":1747055497144,"age":31}
        SingleOutputStreamOperator<JSONObject> DS6 = DS5.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = com.gjn.func.HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        com.gjn.func.HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        String skuId = jsonObject.getString("category1_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_jingnan_guo", "dim_base_category1", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category1_name", skuInfoJsonObj.getString("name"));

                        return a;
                    }
                }
        );
        //DS6.print("ds6-->");

        // kafka 读取 用户表信息
        KafkaSource<String> source3 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("user_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> user = env.fromSource(source3, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //user.print("user->>");

        SingleOutputStreamOperator<JSONObject> userJSON = user.map(JSONObject::parseObject);
        //userJSON.print("userJSON->>");

        //joinAll-->> {"category2_name":"手机通讯","device_35_39":0.05,"search_25_29":0,"orderId":1360,"pv":6,"tm_name":"苹果","search_40_49":0,"category1_name":"手机","md":"SAMSUNG Galaxy S21","search_18_24":0,"orderPrice":8197.0,"id":2188,"search_item":"","skuId":8,"category2_id":"13","search_30_34":0,"create_time":"2025-05-09 20:55:28","os":"Android","device_50":0.03,"ch":"360","device_30_34":0.06,"device_18_24":0.08,"search_50":0,"category1_id":"2","device_25_29":0.07,"tm_id":"2","total_amount":8197.0,"user_id":"28","judge_os":"Android","search_35_39":0,"device_40_49":0.04,"category3_name":"手机","category3_id":"61","ba":"SAMSUNG"}
        // order 宽表 关联 user 宽表
        SingleOutputStreamOperator<JSONObject> joinAll = DS6.keyBy(o -> o.getString("user_id"))
                .intervalJoin(userJSON.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-60), Time.hours(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {
                        jsonObject.putAll(jsonObject2);
                        jsonObject.remove("uid");
                        collector.collect(jsonObject);

                    }
                }).setParallelism(1).uid("intervalJoin new & bloomLog").name("intervalJoin new & bloomLog");
        //joinAll.print("joinAll-->");

        // kafka 读取 日志数据  page-log 表
        //page_log->>> {"common":{"ar":"30","uid":"162","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_443","vc":"v2.1.132","ba":"iPhone","sid":"cc3923ba-0afa-40a8-bb42-c599c9ae4f27"},"page":{"page_id":"order","item":"28,25,10","during_time":6197,"item_type":"sku_ids","last_page_id":"cart"},"ts":1747322289647}
        KafkaSource<String> source4 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> page_log = env.fromSource(source4, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //page_log.print("page_log->>");

        //pageLog-->> {"uid":"43","os":"iOS","ts":1746803851187}
        SingleOutputStreamOperator<JSONObject> pageLog = page_log.map(JSONObject::parseObject);
        //pageLog.print("pageLog-->");

        SingleOutputStreamOperator<JSONObject> pagelog = pageLog.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")){
                    JSONObject common = jsonObject.getJSONObject("common");
                    result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts",jsonObject.getLongValue("ts"));
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo",deviceInfo);
                    if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                            String item = pageInfo.getString("item");
                            result.put("search_item",item);
                        }
                    }
                }
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os",os);
                return result;
            }
        });
        //pagelog.print();

        SingleOutputStreamOperator<JSONObject> processed = joinAll.keyBy(o -> o.getString("user_id"))
                .intervalJoin(pagelog.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-60), Time.hours(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        jsonObject.remove("uid");
                        collector.collect(jsonObject);
                    }
                });
      //processed.print();

        //filter-->> {"uid":"30","search_item":"小米","deviceInfo":{"ar":"3","uid":"30","os":"iOS","ch":"Appstore","md":"iPhone 14 Plus","vc":"v2.1.132","ba":"iPhone"},"ts":1747321162927}
        SingleOutputStreamOperator<JSONObject> filter = pagelog.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject.containsKey("search_item") && !jsonObject.getString("search_item").isEmpty()){
                    return true;
                }
                return false;
            }
        });
        //filter.print("filter-->");

        //过滤出  级别表
        //dic>>>> {"op":"r","after":{"category_name":"优惠","search_category":"性价比","id":4},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"stream_realtime","table":"category_compare_dic","ts_ms":1747204068220}
        SingleOutputStreamOperator<JSONObject> category_compare_dic = kafkaStrDS.map(JSON::parseObject)
                .filter(o->o.getJSONObject("source").getString("table").equals("category_compare_dic"));
        //category_compare_dic.print("dic>>>");

        //categoryDS>>>> {"id":5,"categoryName":"折扣","searchCategory":"性价比"}
        //提取字段
        SingleOutputStreamOperator<JSONObject> categoryDS = category_compare_dic.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                String categoryName = after.getString("category_name");
                String searchCategory = after.getString("search_category");
                object.put("id", id);
                object.put("categoryName", categoryName);
                object.put("searchCategory", searchCategory);
                return object;
            }
        });

        //categoryDS.print("categoryDS>>>");

        //log-->> {"uid":"38","id":22,"search_item":"拯救者","categoryName":"拯救者","searchCategory":"科技与数码","deviceInfo":{"ar":"26","uid":"38","os":"Android","ch":"oppo","md":"xiaomi 13","vc":"v2.1.134","ba":"xiaomi"},"ts":1747322306279}
        SingleOutputStreamOperator<JSONObject> cate_log = filter.keyBy(o -> o.getString("search_item"))
                .intervalJoin(categoryDS.keyBy(o -> o.getString("categoryName")))
                .between(Time.hours(-60), Time.hours(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //cate_log.print("log-->");

        //alljoin-->> {"birthday":"1991-04-09","decade":1990,"category2_name":"电脑整机","gender":"M","orderId":1653,"zodiac_sign":"白羊座","create_ts":1747232160000,"categoryName":"匡威","searchCategory":"时尚与潮流","tm_name":"联想","uid":"51","unit_height":"cm","category1_name":"电脑办公","orderPrice":10599.0,"phone_num":"13499638695","id":28,"search_item":"匡威","skuId":16,"category2_id":"33","email":"e5fw3dv@googlemail.com","height":"181","create_time":"2025-05-16 05:25:31","weight":"89","deviceInfo":{"ar":"7","uid":"51","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.111","ba":"iPhone"},"category1_id":"6","login_name":"e5fw3dv","tm_id":"3","total_amount":57483.1,"user_id":"51","name":"毕辰士","user_level":"1","category3_name":"游戏本","unit_weight":"kg","category3_id":"287","ts_ms":1747204071582,"age":34,"ts":1747315461576}
        SingleOutputStreamOperator<JSONObject> alljoin = processed.keyBy(o -> o.getString("user_id"))
                .intervalJoin(cate_log.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-60), Time.hours(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //alljoin.print("alljoin-->");

//time-->> {"birthday":"1995-06-09","decade":1990,"category2_name":"手机通讯","gender":"F","orderId":1686,"zodiac_sign":"双子座","create_ts":1747232160000,"25-29":"25-29","price_sensitive":"高价商品","tm_name":"苹果","unit_height":"cm","category1_name":"手机","orderPrice":8197.0,"phone_num":"13987845812","id":8,"skuId":9,"category2_id":"13","email":"vrzyir@hotmail.com","ageScore":{"tm_score":0.14,"price_sensitive_score":0.03,"time_period_score":0.07,"category_score":0.12},"height":"166","create_time":"2025-05-16 07:25:46","weight":"45","deviceInfo":{"ar":"26","uid":"8","os":"Android","ch":"xiaomi","md":"vivo x90","vc":"v2.1.134","ba":"vivo"},"category1_id":"2","login_name":"vrzyir","tm_id":"2","total_amount":54804.2,"user_id":"8","age_interval":"25-29","name":"冯嘉","user_level":"1","category3_name":"手机","unit_weight":"kg","category3_id":"61","ts_ms":1747204071581,"age":29,"time_period":"夜间","ts":1747322731503}
        SingleOutputStreamOperator<JSONObject> timePriceMap = alljoin.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject)   {
                if (jsonObject != null) {

                    // 时间段
                    jsonObject.put("time_period", JudgmentFunc.timePeriodJudgment(jsonObject.getLong("ts")));
                    jsonObject.put("age_interval", JudgmentFunc.ageJudgment(jsonObject.getInteger("age")));

                    // 价格敏感度
                    jsonObject.put("price_sensitive", JudgmentFunc.priceCategoryJudgment(jsonObject.getDouble("total_amount")));
                }
                return jsonObject;
            }
        }).setParallelism(1).uid("timePriceMap").name("timePriceMap");

        //timePriceMap.print();

        timePriceMap.map(new pricebase()).print("time-->");
        env.execute();
    }
}