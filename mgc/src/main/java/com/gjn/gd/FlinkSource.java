package com.gjn.gd;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * @Package Utils.FlinkSource
 * @Author jingnan.guo
 * @Date 2025/5/12 10:11
 * @description: 获取数据工具类
 */
public class FlinkSource {
    public static KafkaSource<String> getKafkaSource(String topic) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId("zy")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) {
                        if (bytes != null) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return source;
    }

    public static MySqlSource<String> getmysqlsource(String database, String table) {
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "string");
        properties.setProperty("time.precision.mode", "connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(constant.MYSQL_HOST)
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .port(constant.MYSQL_PORT)
                .databaseList()
                .tableList(database + "." + table)
                .username(constant.MYSQL_USER_NAME)
                .password(constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return mySqlSource;
    }

}
