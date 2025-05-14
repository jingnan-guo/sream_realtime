package com.gjn.gd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.base.DimBaseCategory;
import com.gjn.func.MapDeviceAndSearchMarkModelFunc;
import com.gjn.func.ProcessFilterRepeatTsDataFunc;
import com.gjn.func.processfilter;
import com.gjn.utils.JdbcUtils;
import com.gjn.utils.flinksink;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Package com.gjn.dwd.dwduser
 * @Author jingnan.guo
 * @Date 2025/5/13 10:21
 * @description:  处理页面日志信息  处理 设备 求出 相应  分数
 */
public class dwduser {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;
    private static final double device_rate_weight_coefficient = 0.1;
    private static final double search_rate_weight_coefficient = 0.15;

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306/stream_realtime?useSSL=false",
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from stream_realtime.base_category3 as b3  \n" +
                    "     join stream_realtime.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join stream_realtime.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> ste = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> stre = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        SingleOutputStreamOperator<JSONObject> user = stre.map(jsonStr -> {
            JSONObject json = JSON.parseObject(String.valueOf(jsonStr));
            JSONObject after = json.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer epochDay = after.getInteger("birthday");
                if (epochDay != null) {
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));

                    String zodiacSign = getZodiacSign(date);
                    after.put("zodiac_sign", zodiacSign);
                    int year = date.getYear();
                    int decade = (year / 10) * 10; // 计算年代（如1990, 2000）
                    after.put("decade", decade);

                    LocalDate currentDate = LocalDate.now();
                    int age = calculateAge(date, currentDate);
                    after.put("age", age);

                }
            }
            return json;
        });


        SingleOutputStreamOperator<JSONObject> userK = user.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null){
                    JSONObject after = jsonObject.getJSONObject("after");
                    String birthday = after.getString("birthday");

                    String name = after.getString("name");
                    String zodiacSign = after.getString("zodiac_sign");
                    String id = after.getString("id");
                    Integer birthDecade = after.getInteger("decade");
                    String login_name = after.getString("login_name");
                    String userLevel = after.getString("user_level");
                    String phoneNum = after.getString("phone_num");
                    String email = after.getString("email");
                    Long tsMs = jsonObject.getLong("ts_ms");
                    Integer age = after.getInteger("age");

                    object.put("birthday", birthday);
                    object.put("decade", birthDecade);
                    object.put("name", name);
                    object.put("zodiac_sign", zodiacSign);
                    object.put("id", id);
                    object.put("login_name", login_name);
                    object.put("user_level", userLevel);
                    object.put("phone_num", phoneNum);
                    object.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                    object.put("email", email);
                    object.put("ts_ms",tsMs);
                    object.put("age", age);
                }

                return object;
            }
        });


        SingleOutputStreamOperator<JSONObject> sup = ste.map(JSON::parseObject).filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        SingleOutputStreamOperator<JSONObject> supK = sup.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null){
                    JSONObject after = jsonObject.getJSONObject("after");
                    String uid = after.getString("uid");
                    String height = after.getString("height");
                    String weight = after.getString("weight");
                    String unitWeight = after.getString("unit_weight");
                    String unitHeight = after.getString("unit_height");
                    object.put("uid", uid);
                    object.put("height", height);
                    object.put("weight", weight);
                    object.put("unit_weight", unitWeight);
                    object.put("unit_height", unitHeight);
                }
                return object;
            }
        });


        SingleOutputStreamOperator<JSONObject> ds3 = userK.keyBy(o -> o.getString("id"))
                .intervalJoin(supK.keyBy(o -> o.getString("uid")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject.getString("id").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject);
                            result.put("height",jsonObject2.getString("height"));
                            result.put("unit_height",jsonObject2.getString("unit_height"));
                            result.put("weight",jsonObject2.getString("weight"));
                            result.put("unit_weight",jsonObject2.getString("unit_weight"));
                        }
                        collector.collect(result);
                    }
                });
//        ds3.print();
//        ds3.map(o -> JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_user_log"));


//      page日志信息
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkalog = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> logJson = kafkalog.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> pagelog = logJson.map(new RichMapFunction<JSONObject, JSONObject>() {
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
//        pagelog.print();
        SingleOutputStreamOperator<JSONObject> filtered = pagelog.filter(o -> !o.getString("uid").isEmpty());
//        filtered.print();
        KeyedStream<JSONObject, String> keyedSteamLogPage = filtered.keyBy(o -> o.getString("uid"));
//        keyedSteamLogPage.print();
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedSteamLogPage.process(new ProcessFilterRepeatTsDataFunc());
//        processStagePageLogDs.print();

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(o -> o.getString("uid")).
                process(new processfilter())
                .keyBy(o -> o.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
//        win2MinutesPageLogsDs.print();
        //{"device_35_39":0.04,"os":"iOS,Android","device_50":0.02,"search_25_29":0,"ch":"Appstore,360","pv":6,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"83","device_25_29":0.06,"md":"iPhone 14,xiaomi 13 Pro ","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone,xiaomi","search_30_34":0}
        SingleOutputStreamOperator<JSONObject> DS1 = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));

        SingleOutputStreamOperator<String> DS1String = DS1.map(o -> JSONObject.toJSONString(o));

        //{"birthday":"1998-10-09","decade":1990,"gender":"M","zodiac_sign":"天秤座","create_ts":1747083896000,"weight":"77","uid":102,"login_name":"t9ao3sf","unit_height":"cm","name":"孙力","user_level":"1","phone_num":"13326899233","id":102,"unit_weight":"kg","email":"t9ao3sf@yahoo.com","ts_ms":1747055497144,"age":26,"height":"154"}
        DS1String.print();
        // 将用户信息表 存入kafka主题
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic_userInfo")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DS1String.sinkTo(sink);



        env.execute();
    }

    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

// 定义星座区间映射
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
// 如果生日日期晚于当前日期，抛出异常
        if (birthDate.isAfter(currentDate)) {
            throw new IllegalArgumentException("生日日期不能晚于当前日期");
        }

        int age = currentDate.getYear() - birthDate.getYear();

// 如果当前月份小于生日月份，或者月份相同但日期小于生日日期，则年龄减1
        if (currentDate.getMonthValue() < birthDate.getMonthValue() ||
                (currentDate.getMonthValue() == birthDate.getMonthValue() &&
                        currentDate.getDayOfMonth() < birthDate.getDayOfMonth())) {
            age--;
        }

        return age;
    }

}
