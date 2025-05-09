package com.gjn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.func.FilterBloomDeduplicatorFunc;
import com.gjn.func.MapCheckRedisSensitiveWordsFunc;
import com.gjn.utils.EnvironmentSettingUtils;
import com.gjn.utils.KafkaUtils;
import com.gjn.utils.finksink;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.List;

/**
 * @Package com.cj.asd.bbb
 * @Author chen.jian
 * @Date 2025/5/8 19:00
 * @description:
 */
public class mgc_v2 {
    private static final String kafka_botstrap_servers = "cdh01:9092";
    private static final String kafka_db_fact_comment_topic = "fact_comment";
    private static final String kafka_result_sensitive_words_topic = "words_comment_topic";

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_db_fact_comment_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        ).uid("kafka_fact_comment_source").name("kafka_fact_comment_source");
        //kafkaCdcDbSource.print();

        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");

        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");

        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        }).uid("second sensitive word check").name("second sensitive word check");
        secondCheckMap.print();

        secondCheckMap.map(data -> data.toJSONString())
                .sinkTo(
                        finksink.getDorisSink("result_sensitive_words_user")
                )
                .uid("sink to kafka result sensitive words topic")
                .name("sink to kafka result sensitive words topic");


        env.execute();
    }
}
