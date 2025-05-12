package com.gjn.gd;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.gjn.ods.flinkcdc
 * @Author jingnan.guo
 * @Date 2025/5/12 9:03
 * @description: flinkCDC 读取mysql 数据
 */
public class flinkcdc {
    public static void main(String[] args) throws Exception {

        // 初始化 Properties 对象，用于配置 Debezium
        Properties prop=new Properties();
        // 设置 MySQL 连接属性：关闭 SSL
        prop.put("useSSL","false");
        // 设置 Decimal 处理模式为 double
        prop.put("decimal.handling.mode","double");
        // 设置时间精度模式
        prop.put("time.precision.mode","connect");
        // 设置增量快照的分块键列
        prop.setProperty("scan.incremental.snapshot.shunk.key-column","id");
        // 构建 MySqlSource 对象，用于捕获 MySQL 数据库的变化
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03") // 设置 MySQL 主机名
                .port(3306) // 设置 MySQL 端口
                .databaseList("stream_realtime") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("stream_realtime.*") // 设置捕获的表
                .username("root") // 设置 MySQL 用户名
                .password("root") // 设置 MySQL 密码
                .debeziumProperties(prop)// 设置 Debezium 属性
                .startupOptions(StartupOptions.initial()) // 设置启动选项为初始状态
                //.startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        // 获取 Flink 的 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);
        // 从 MySQL 数据源创建数据流
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mySQLSource.print();
        // 构建 KafkaSink 对象，用于将数据流发送到 Kafka 集群
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        // 将数据流发送到 Kafka Sink
        mySQLSource.sinkTo(sink);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
