package ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package ods.Flink_Cdc
 * @Author zeyang_jia
 * @Date 2025/5/6 18:11
 * @description: 采集业务数据
 */
public class Flink_Cdc {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("yourHostname")
                .port(3306)
                .databaseList("yourDatabaseName") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("yourDatabaseName.yourTableName") // 设置捕获的表
                .username("yourUsername")
                .password("yourPassword")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4)
                .print().setParallelism(1); // 设置 sink 节点并行度为 1

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
