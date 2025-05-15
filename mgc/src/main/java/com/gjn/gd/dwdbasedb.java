package com.gjn.gd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.gjn.dwd.dwdbasedb
 * @Author jingnan.guo
 * @Date 2025/5/12 11:17
 * @description:
 */
public class dwdbasedb {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 构建Kafka数据源：
         * - 连接cdh01:9092服务器
         * - 订阅topic_db主题
         * - 使用my-group消费者组
         * - 从最早偏移量开始消费
         * - 使用字符串反序列化器
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> ste = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        /**
         * 数据过滤与解析：
         * 1. 将JSON字符串转换为JSON对象
         * 2. 过滤出source.table字段为"user_info"的数据
         */
        SingleOutputStreamOperator<JSONObject> stre = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        /**
         * 用户信息处理：
         * 1. 解析after字段中的生日数据
         * 2. 将EpochDay格式转换为ISO日期格式
         * 3. 计算星座、年代、年龄等衍生字段
         * 4. 更新原始JSON对象
         */
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

        /**
         * 用户信息字段提取：
         * 从JSON对象中提取核心用户属性字段
         * 包含基础信息和计算字段：生日、年代、姓名、星座、ID等
         */
        SingleOutputStreamOperator<JSONObject> userK = user.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");

                String birthday = after.getString("birthday");
                String gender = after.getString("gender");
                String name = after.getString("name");
                String zodiacSign = after.getString("zodiac_sign");
                Integer id = after.getInteger("id");
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
                object.put("gender", gender);
                object.put("email", email);
                object.put("ts_ms",tsMs);
                object.put("age", age);
                return object;
            }
        });

        /**
         * 补充信息过滤：
         * 1. 解析JSON数据
         * 2. 过滤出source.table字段为"user_info_sup_msg"的数据
         */
        SingleOutputStreamOperator<JSONObject> sup = ste.map(JSON::parseObject).filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
        /**
         * 补充信息字段提取：
         * 提取用户扩展属性字段
         * 包含身高、体重、单位、创建时间等
         */
        SingleOutputStreamOperator<JSONObject> supK = sup.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer uid = after.getInteger("uid");
                String height = after.getString("height");
                String weight = after.getString("weight");
                String unitWeight = after.getString("unit_weight");
                String unitHeight = after.getString("unit_height");
                Long createTs = after.getLong("create_ts");
                object.put("uid", uid);
                object.put("height", height);
                object.put("weight", weight);
                object.put("unit_weight", unitWeight);
                object.put("unit_height", unitHeight);
                object.put("create_ts", createTs);
                return object;
            }
        });
        /**
         * 间隔连接操作：
         * 1. 按用户ID关联主信息和补充信息
         * 2. 在60秒时间窗口内进行关联
         * 3. 合并两个数据流的字段
         */
        SingleOutputStreamOperator<JSONObject> ds3 = userK.keyBy(o -> o.getInteger("id"))
                .intervalJoin(supK.keyBy(o -> o.getInteger("uid")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector){
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });

        //ds3.print();
        // 将合并后的JSON对象转换为字符串格式
        SingleOutputStreamOperator<String> ds3String = ds3.map(o -> JSONObject.toJSONString(o));

        //{"birthday":"1998-10-09","decade":1990,"gender":"M","zodiac_sign":"天秤座","create_ts":1747083896000,"weight":"77","uid":102,"login_name":"t9ao3sf","unit_height":"cm","name":"孙力","user_level":"1","phone_num":"13326899233","id":102,"unit_weight":"kg","email":"t9ao3sf@yahoo.com","ts_ms":1747055497144,"age":26,"height":"154"}
        ds3String.print();
        // 将用户信息表 存入kafka主题
        /**
         * Kafka输出配置：
         * 1. 连接cdh01:9092服务器
         * 2. 写入user_info主题
         * 3. 使用至少一次语义保证
         */
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("user_info")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        //存入kafka
        ds3String.sinkTo(sink);


        env.execute();
    }

    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

// 定义星座区间映射
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        }
        return "未知"; // 默认情况，实际上不会执行到这一步
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
