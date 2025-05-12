package ODS;

import Constant.Constant;
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
 * @Package ODS.Flink_Kafka
 * @Author zeyang_jia
 * @Date 2025/5/12 8:55
 * @description: 采集业务数据
 */
public class Flink_Kafka {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");
//        properties.setProperty("scan.incremental.snapshot.chunk.key-column", "gmall_config.base_region");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.mysql_host)
//                .startupOptions(StartupOptions.earliest())
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(properties)
                .port(Constant.MYSQL_PORT)
                .databaseList(Constant.mysql_db)
                .tableList(Constant.mysql_db+".*")
                .username(Constant.mysql_user_name)
                .password(Constant.mysql_password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);// 设置 sink 节点并行度为 1
        //c 添加
        //d 删除
        //修改 u
        //r 读
        ds.print();
//        15> {"before":null,"after":{"id":358,"order_id":185,"order_status":"1005","create_time":1747092546000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747012788000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":2567538,"row":0,"thread":31,"query":null},"op":"c","ts_ms":1747012786927,"transaction":null}
//        13> {"before":null,"after":{"id":1277,"log":"{\"common\":{\"ar\":\"10\",\"ba\":\"vivo\",\"ch\":\"oppo\",\"is_new\":\"1\",\"md\":\"vivo x90\",\"mid\":\"mid_85\",\"os\":\"Android 12.0\",\"sid\":\"467f61f3-0971-40d0-8b69-3cf8debda10b\",\"uid\":\"103\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"13\",\"item_type\":\"sku_id\",\"pos_id\":6,\"pos_seq\":0},{\"item\":\"2\",\"item_type\":\"sku_id\",\"pos_id\":6,\"pos_seq\":1},{\"item\":\"14\",\"item_type\":\"sku_id\",\"pos_id\":6,\"pos_seq\":2},{\"item\":\"16\",\"item_type\":\"sku_id\",\"pos_id\":6,\"pos_seq\":3}],\"page\":{\"during_time\":14266,\"last_page_id\":\"payment\",\"page_id\":\"mine\"},\"ts\":1747063734780}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747012788000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"z_log","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":2565964,"row":0,"thread":31,"query":null},"op":"c","ts_ms":1747012786798,"transaction":null}
//        11> {"before":{"id":185,"consignee":"窦善厚","consignee_tel":"13644859133","total_amount":"13707.00","order_status":"1001","user_id":103,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"176797378529636","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","create_time":1747092489000,"operate_time":null,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":10,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"13707.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747697289000},"after":{"id":185,"consignee":"窦善厚","consignee_tel":"13644859133","total_amount":"13707.00","order_status":"1002","user_id":103,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"176797378529636","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","create_time":1747092489000,"operate_time":1747092521000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":10,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"13707.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747697289000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747012788000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_info","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":2564963,"row":0,"thread":31,"query":null},"op":"u","ts_ms":1747012786791,"transaction":null}
//        12> {"before":null,"after":{"id":357,"order_id":185,"order_status":"1002","create_time":1747092521000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747012788000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":2565669,"row":0,"thread":31,"query":null},"op":"c","ts_ms":1747012786795,"transaction":null}
//        10> {"before":{"id":185,"out_trade_no":"176797378529636","order_id":185,"user_id":103,"payment_type":"1103","trade_no":null,"total_amount":"13707.00","subject":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","payment_status":"1601","create_time":1747092498000,"callback_time":null,"callback_content":null,"operate_time":null},"after":{"id":185,"out_trade_no":"176797378529636","order_id":185,"user_id":103,"payment_type":"1103","trade_no":null,"total_amount":"13707.00","subject":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","payment_status":"1602","create_time":1747092498000,"callback_time":1747092521000,"callback_content":"callback xxxxxxx","operate_time":1747092521000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747012788000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"payment_info","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":2564295,"row":0,"thread":31,"query":null},"op":"u","ts_ms":1747012786786,"transaction":null}


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.kafka_brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(Constant.topic_db)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        ds.sinkTo(sink);
        env.execute("Print MySQL Snapshot + Binlog");
    }

}
