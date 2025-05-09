package SensitiveWordFiltering;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import constat.constat;
import func.FilterBloomDeduplicatorFunc;
import func.MapCheckRedisSensitiveWordsFunc;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @Package SensitiveWordFiltering.DbusBanBlackListUserInfo2Kafka
 * @Author zeyang_jia
 * @Date 2025/5/8 11:10
 * @description: 黑名单封禁 Task 04
 * @Test
 * DataStreamSource<String> kafkaCdcDbSource = env.socketTextStream("127.0.0.1", 9999);
 */
public class DbusBanBlackListUserInfo2Kafka {

//    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
//    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");
//    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka.result.sensitive.words.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        "cdh01:9092",
                        "db_fact_comment_topic",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        ).uid("kafka_fact_comment_source").name("kafka_fact_comment_source");
//        kafkaCdcDbSource.print();


        // {"info_original_total_amount":"24038.00","info_activity_reduce_amount":"0.00","commentTxt":"\n\n小米电视4A屏幕显示效果一般，建议谨慎购买。","info_province_id":14,"info_payment_way":"3501","ds":"20250506","info_create_time":1746543508000,"info_refundable_time":1747148308000,"info_order_status":"1001","id":68,"spu_id":6,"table":"comment_info","info_tm_ms":1746518018698,"op":"c","create_time":1746543560000,"info_user_id":26,"info_op":"c","info_trade_body":"小米电视4A 70英寸 4K超高清 HDR 二级能效 2GB+16GB L70M5-4A 内置小爱 智能网络液晶平板教育电视等8件商品","sku_id":21,"server_id":"1","dic_name":"好评","info_consignee_tel":"13477763374","info_total_amount":"23968.00","info_out_trade_no":"443513674664624","appraise":"1201","user_id":26,"info_id":836,"info_coupon_reduce_amount":"70.00","order_id":836,"info_consignee":"穆素云","ts_ms":1746518019174,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");
//        mapJsonStr.print();

        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000, 0.01));
//        bloomFilterDs.print();

        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");
//        SensitiveWordsDs.print();



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
        SingleOutputStreamOperator<String> map = secondCheckMap.map(JSON::toString);

       //写入doris
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("cdh03:8110")
                        .setTableIdentifier(constat.DORIS_DATABASE + "." + "result_sensitive_words_user")
                        .setUsername("root")
                        .setPassword("123456")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(5*1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        map.sinkTo(sink);
// 数据格式
//        13> {"msg":"购买体验极差，商品质量差，小孔全面屏显示效果差，4800万超清四摄拍照模糊，5020mAh大电量待机时间短，128GB大存储不够用，8GB+128GB选择困难。",
//        "consignee":"余华慧",
//        "violation_grade":"","
//        user_id":2304,
//        "violation_msg":"","
//        is_violation":0,
//        "ts_ms":1746712419527,
//        "ds":"20250508"}

//        secondCheckMap.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink("cdh01:9092", "result_sensitive_words_topic")
//                        )
//                        .uid("sink to kafka result sensitive words topic")
//                        .name("sink to kafka result sensitive words topic");

        env.execute();
    }
}
