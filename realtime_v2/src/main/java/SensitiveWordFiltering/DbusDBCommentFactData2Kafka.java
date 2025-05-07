package SensitiveWordFiltering;
import Base.BaseApp;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import constat.constat;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.AsyncHbaseDimBaseDicFunc;
import utils.SensitiveWordsUtils;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @Package SensitiveWordFiltering.DbusDBCommentFactData2Kafka
 * @Author zeyang_jia
 * @Date 2025/5/7 15:35
 * @description: 取数据
 */
public class DbusDBCommentFactData2Kafka extends BaseApp {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    public static void main(String[] args) throws Exception {
        new DbusDBCommentFactData2Kafka().start(10020,4,"DbusDBCommentFactData2Kafka", constat.TOPIC_DB);

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        DataStream<JSONObject> kafkaStrDS2 =kafkaStrDS.map(JSON::parseObject).filter(jsonObject -> jsonObject.getJSONObject("source")
                .getLong("tm_ms")!=null);

        DataStream<String> kafkaStrDS3 = kafkaStrDS2.map(new RichMapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toString();
            }
        });

        DataStream<JSONObject> common_info=kafkaStrDS3.map(JSON::parseObject).filter(jsonObject -> jsonObject.getJSONObject("source")
               .getString("table").equals("common_info"));

        DataStream<com.alibaba.fastjson.JSONObject> filteredOrderInfoStream = kafkaStrDS
                .map(com.alibaba.fastjson.JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");

        DataStream<com.alibaba.fastjson.JSONObject> filteredStream = kafkaStrDS
                .map(com.alibaba.fastjson.JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));

        DataStream<com.alibaba.fastjson.JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        100
                )
                .uid("async_hbase_dim_base_dic_func")
                .name("async_hbase_dim_base_dic_func");
//        3> {"op":"c","after":{"create_time":1745881413000,"user_id":85,"appraise":"1201","comment_txt":"评论内容：92891936338449143531178212411794869843252711287996","nick_name":"琳琳","sku_id":8,"id":16,"spu_id":3,"order_id":174,"dic_name":"N/A"},"source":{"thread":306,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000029","connector":"mysql","pos":12677313,"name":"mysql_binlog_source","row":0,"ts_ms":1746172340000,"snapshot":"false","db":"gmall_config","table":"comment_info"},"ts_ms":1746172339412}
//        enrichedStream.print();

        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<com.alibaba.fastjson.JSONObject, com.alibaba.fastjson.JSONObject>() {
                    @Override
                    public com.alibaba.fastjson.JSONObject map(com.alibaba.fastjson.JSONObject jsonObject){
                        com.alibaba.fastjson.JSONObject resJsonObj = new com.alibaba.fastjson.JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        com.alibaba.fastjson.JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            com.alibaba.fastjson.JSONObject after = jsonObject.getJSONObject("after");
                            resJsonObj.put("ts_ms", tsMs);
                            resJsonObj.put("db", dbName);
                            resJsonObj.put("table", tableName);
                            resJsonObj.put("server_id", serverId);
                            resJsonObj.put("appraise", after.getString("appraise"));
                            resJsonObj.put("commentTxt", after.getString("comment_txt"));
                            resJsonObj.put("op", jsonObject.getString("op"));
                            resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                            resJsonObj.put("create_time", after.getLong("create_time"));
                            resJsonObj.put("user_id", after.getLong("user_id"));
                            resJsonObj.put("sku_id", after.getLong("sku_id"));
                            resJsonObj.put("id", after.getLong("id"));
                            resJsonObj.put("spu_id", after.getLong("spu_id"));
                            resJsonObj.put("order_id", after.getLong("order_id"));
                            resJsonObj.put("dic_name", after.getString("dic_name"));
                            return resJsonObj;
                        }
                        return null;
                    }
                })
                .uid("map-order_comment_data")
                .name("map-order_comment_data");
//        3> {"op":"c","create_time":1745875822000,"commentTxt":"评论内容：57871611184688355288571831153868498236237158711252","sku_id":28,"server_id":"1","dic_name":"N/A","appraise":"1201","user_id":115,"id":47,"spu_id":9,"order_id":327,"ts_ms":1746172375019,"db":"gmall_config","table":"comment_info"}

        orderCommentMap.print();


        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<com.alibaba.fastjson.JSONObject, com.alibaba.fastjson.JSONObject>() {
            @Override
            public com.alibaba.fastjson.JSONObject map(com.alibaba.fastjson.JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                com.alibaba.fastjson.JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                com.alibaba.fastjson.JSONObject resultObj = new com.alibaba.fastjson.JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map-order_info_data").name("map-order_info_data");
//        orderInfoMapDs.print();
//        3> {"payment_way":"3501","refundable_time":1746918821000,"original_total_amount":"7217.00","order_status":"1002","consignee_tel":"13261885666","trade_body":"十月稻田 辽河长粒香 东北大米 5kg等5件商品","id":183,"operate_time":1746314067000,"op":"u","consignee":"郑娟","create_time":1746314021000,"coupon_reduce_amount":"0.00","out_trade_no":"528234341765812","total_amount":"6717.00","user_id":78,"province_id":10,"tm_ms":1746254329845,"activity_reduce_amount":"500.00"}
//

        // orderCommentMap.order_id join orderInfoMapDs.id
        KeyedStream<com.alibaba.fastjson.JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<com.alibaba.fastjson.JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));

        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"评论内容：52198813817222113474133821791377912858419193882331","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1002","id":84,"spu_id":3,"table":"comment_info","info_tm_ms":1746596796189,"info_operate_time":1746624052000,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":11,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796318,"db":"realtime_v1"}
//        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
//                .between(Time.minutes(-1), Time.minutes(1))
//                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
//                .uid("interval_join_order_comment_and_order_info_func").name("interval_join_order_comment_and_order_info_func");
//
//
//        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
//        // {"info_original_total_amount":"1299.00","info_activity_reduce_amount":"0.00","commentTxt":"\n\n这款Redmi 10X虽然价格亲民，但续航能力一般且相机效果平平，在同类产品中竞争力不足。","info_province_id":32,"info_payment_way":"3501","info_create_time":1746566254000,"info_refundable_time":1747171054000,"info_order_status":"1004","id":75,"spu_id":2,"table":"comment_info","info_tm_ms":1746518021300,"info_operate_time":1746563573000,"op":"c","create_time":1746563573000,"info_user_id":149,"info_op":"u","info_trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 明月灰 游戏智能手机 小米 红米等1件商品","sku_id":7,"server_id":"1","dic_name":"好评","info_consignee_tel":"13144335624","info_total_amount":"1299.00","info_out_trade_no":"199223184973112","appraise":"1201","user_id":149,"info_id":327,"info_coupon_reduce_amount":"0.00","order_id":327,"info_consignee":"范琳","ts_ms":1746518021294,"db":"realtime_v1"}
//        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<com.alibaba.fastjson.JSONObject, com.alibaba.fastjson.JSONObject>() {
//            @Override
//            public com.alibaba.fastjson.JSONObject map(com.alibaba.fastjson.JSONObject jsonObject) {
//                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
//                return jsonObject;
//            }
//        }).uid("map-generate_comment").name("map-generate_comment");
//
//
//        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"\n\n差评：续航差、相机效果一般,高自联","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1001","id":88,"spu_id":5,"table":"comment_info","info_tm_ms":1746596795948,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"c","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":19,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796327,"db":"realtime_v1"}
//        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<com.alibaba.fastjson.JSONObject, com.alibaba.fastjson.JSONObject>() {
//            private transient Random random;
//
//            @Override
//            public void open(Configuration parameters){
//                random = new Random();
//            }
//
//            @Override
//            public com.alibaba.fastjson.JSONObject map(com.alibaba.fastjson.JSONObject jsonObject){
//                if (random.nextDouble() < 0.2) {
//                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
//                    System.err.println("change commentTxt: " + jsonObject);
//                }
//                return jsonObject;
//            }
//        }).uid("map-sensitive-words").name("map-sensitive-words");
//
//        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<com.alibaba.fastjson.JSONObject, com.alibaba.fastjson.JSONObject>() {
//            @Override
//            public com.alibaba.fastjson.JSONObject map(com.alibaba.fastjson.JSONObject jsonObject){
//                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
//                return jsonObject;
//            }
//        }).uid("add json ds").name("add json ds");
//
//        suppleTimeFieldDs.map(js -> js.toJSONString())
//                .sinkTo(
//                        KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_db_fact_comment_topic)
//                ).uid("kafka_db_fact_comment_sink").name("kafka_db_fact_comment_sink");
//
//

    }

    }

