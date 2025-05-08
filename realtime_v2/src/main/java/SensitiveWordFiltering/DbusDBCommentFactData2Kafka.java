package SensitiveWordFiltering;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import constat.constat;
import func.IntervalJoinOrderCommentAndOrderInfoFunc;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.retailersv1.DbusDBCommentFactData2Kafka
 * @Author zeyang_jia
 * @Date 2025/5/7 18:44
 * @description: Read MySQL CDC binlog data to kafka topics Task-03
 */
public class DbusDBCommentFactData2Kafka {

    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        // 评论表 取数
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        "cdh01:9092",
                        constat.TOPIC_DB,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                        }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");
//        kafkaCdcDbSource.print();

        // 订单主表
        // {"op":"c","after":{"payment_way":"3501","consignee":"窦先敬","create_time":1746660084000,"refundable_time":1747264884000,"original_total_amount":"6499.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"929361421194788","total_amount":"5999.00","user_id":184,"province_id":23,"consignee_tel":"13879332785","trade_body":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机等1件商品","id":1123,"activity_reduce_amount":"500.00"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":31381479,"name":"mysql_binlog_source","thread":20265,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596800000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596800483}
        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");
//        filteredOrderInfoStream.print();

        // 评论表进行进行升维处理 和hbase的维度进行关联补充维度数据
        // {"op":"c","after":{"create_time":1746624077000,"user_id":178,"appraise":"1201","comment_txt":"评论内容：44237268662145286925725839461514467765118653811952","nick_name":"珠珠","sku_id":14,"id":85,"spu_id":4,"order_id":1010},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":30637591,"name":"mysql_binlog_source","thread":20256,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596796000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746596796319}
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));
//        3> {"op":"u","before":{"payment_way":"3501","refundable_time":1747264165000,"original_total_amount":"12787.00","order_status":"1002","consignee_tel":"13937298935","trade_body":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml等3件商品","id":3906,"operate_time":1746659402000,"consignee":"呼延行","create_time":1746659365000,"coupon_reduce_amount":"0.00","out_trade_no":"129916364745444","total_amount":"11587.10","user_id":1130,"province_id":12,"activity_reduce_amount":"1199.90"},"after":{"payment_way":"3501","refundable_time":1747264165000,"original_total_amount":"12787.00","order_status":"1005","consignee_tel":"13937298935","trade_body":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml等3件商品","id":3906,"operate_time":1746659425000,"consignee":"呼延行","create_time":1746659365000,"coupon_reduce_amount":"0.00","out_trade_no":"129916364745444","total_amount":"11587.10","user_id":1130,"province_id":12,"activity_reduce_amount":"1199.90"},"source":{"thread":400,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000039","connector":"mysql","pos":13647560,"name":"mysql_binlog_source","row":0,"ts_ms":1746671651000,"snapshot":"false","db":"gmall_config","table":"order_info"},"ts_ms":1746671650438}
//

        // {"op":"c","after":{"create_time":1746568494000,"user_id":126,"appraise":"1202","comment_txt":"评论内容：43341158654483726916799957869464279782846343359228","nick_name":"琬琬","sku_id":5,"id":77,"spu_id":2,"order_id":334,"dic_name":"中评"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":29984187,"name":"mysql_binlog_source","thread":19913,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746518022000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746518022747}
        DataStream<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        120,
                        TimeUnit.SECONDS,
                        100
                ).uid("async_hbase_dim_base_dic_func")
                .name("async_hbase_dim_base_dic_func");
//        3> {"op":"u","before":{"payment_way":"3501","refundable_time":1747264165000,"original_total_amount":"12787.00","order_status":"1005","consignee_tel":"13937298935","trade_body":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml等3件商品","id":3906,"operate_time":1746659425000,"consignee":"呼延行","create_time":1746659365000,"coupon_reduce_amount":"0.00","out_trade_no":"129916364745444","total_amount":"11587.10","user_id":1130,"province_id":12,"activity_reduce_amount":"1199.90"},"after":{"payment_way":"3501","refundable_time":1747264165000,"original_total_amount":"12787.00","order_status":"1006","consignee_tel":"13937298935","trade_body":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml等3件商品","id":3906,"operate_time":1746663030000,"consignee":"呼延行","create_time":1746659365000,"coupon_reduce_amount":"0.00","out_trade_no":"129916364745444","total_amount":"11587.10","user_id":1130,"province_id":12,"activity_reduce_amount":"1199.90"},"source":{"thread":400,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000039","connector":"mysql","pos":13650954,"name":"mysql_binlog_source","row":0,"ts_ms":1746671651000,"snapshot":"false","db":"gmall_config","table":"order_info"},"ts_ms":1746671650605}
//
//        enrichedStream.print();
//
//        // {"op":"c","create_time":1746653124000,"commentTxt":"评论内容：36913887965764674188858298813931966419435136341364","sku_id":19,"server_id":"1","dic_name":"好评","appraise":"1201","user_id":221,"id":89,"spu_id":5,"order_id":979,"ts_ms":1746596800251,"db":"realtime_v1","table":"comment_info"}
        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject resJsonObj = new JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
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
//        10> {"op":"c","create_time":1746656278000,"commentTxt":"评论内容：44516771592496285853233438719255515716333265222641","sku_id":7,"server_id":"1","dic_name":"N/A","appraise":"1201","user_id":1246,"id":472,"spu_id":2,"order_id":4276,"ts_ms":1746672903941,"db":"gmall_config","table":"comment_info"}

//        orderCommentMap.print();









        // {"op":"c","payment_way":"3501","consignee":"张贞","create_time":1746653800000,"refundable_time":1747258600000,"original_total_amount":"69.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"914927687659481","total_amount":"69.00","user_id":156,"province_id":10,"tm_ms":1746596799810,"consignee_tel":"13114791128","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":1108,"activity_reduce_amount":"0.00"}
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map-order_info_data").name("map-order_info_data");
//        3> {"payment_way":"3501","refundable_time":1747265047000,"original_total_amount":"9799.00","order_status":"1002","consignee_tel":"13338291277","trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰等1件商品","id":4282,"operate_time":1746660280000,"op":"u","consignee":"顾海","create_time":1746660247000,"coupon_reduce_amount":"0.00","out_trade_no":"796675798295765","total_amount":"9549.00","user_id":1249,"province_id":16,"tm_ms":1746672904062,"activity_reduce_amount":"250.00"}

//        orderInfoMapDs.print();

//
//        // orderCommentMap.order_id join orderInfoMapDs.id
        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));

//        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"评论内容：52198813817222113474133821791377912858419193882331","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1002","id":84,"spu_id":3,"table":"comment_info","info_tm_ms":1746596796189,"info_operate_time":1746624052000,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":11,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796318,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
                .uid("interval_join_order_comment_and_order_info_func").name("interval_join_order_comment_and_order_info_func");
//        orderMsgAllDs.print();


//        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
//        // {"info_original_total_amount":"1299.00","info_activity_reduce_amount":"0.00","commentTxt":"\n\n这款Redmi 10X虽然价格亲民，但续航能力一般且相机效果平平，在同类产品中竞争力不足。","info_province_id":32,"info_payment_way":"3501","info_create_time":1746566254000,"info_refundable_time":1747171054000,"info_order_status":"1004","id":75,"spu_id":2,"table":"comment_info","info_tm_ms":1746518021300,"info_operate_time":1746563573000,"op":"c","create_time":1746563573000,"info_user_id":149,"info_op":"u","info_trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 明月灰 游戏智能手机 小米 红米等1件商品","sku_id":7,"server_id":"1","dic_name":"好评","info_consignee_tel":"13144335624","info_total_amount":"1299.00","info_out_trade_no":"199223184973112","appraise":"1201","user_id":149,"info_id":327,"info_coupon_reduce_amount":"0.00","order_id":327,"info_consignee":"范琳","ts_ms":1746518021294,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        }).uid("map-generate_comment").name("map-generate_comment");
//        supplementDataMap.print();
//        1> {"info_original_total_amount":"7338.00","info_activity_reduce_amount":"669.90","commentTxt":"TCL 65Q10电视画质差，音响效果一般，操作界面复杂，不支持语音控制。","info_province_id":6,"info_payment_way":"3501","info_refundable_time":1747247771000,"info_order_status":"1004","info_create_time":1746642971000,"id":366,"spu_id":11,"table":"comment_info","info_operate_time":1746643016000,"info_tm_ms":1746671581129,"op":"c","create_time":1746643016000,"info_user_id":166,"info_op":"u","info_trade_body":"TCL 65Q10 65英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等4件商品","sku_id":32,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13799635246","info_total_amount":"6668.10","info_out_trade_no":"367173324932196","appraise":"1201","user_id":166,"info_id":3456,"info_coupon_reduce_amount":"0.00","order_id":3456,"info_consignee":"和素云","ts_ms":1746671581117,"db":"gmall_config"}

//        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"\n\n差评：续航差、相机效果一般,高自联","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1001","id":88,"spu_id":5,"table":"comment_info","info_tm_ms":1746596795948,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"c","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":19,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796327,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters){
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject){
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("map-sensitive-words").name("map-sensitive-words");
//        suppleMapDs.print();
//        change commentTxt: {"info_original_total_amount":"16463.00","info_activity_reduce_amount":"0.00","commentTxt":"Apple iPhone 12 64GB 蓝色，支持5G双卡双待，质量一般，电池续航差。,供应麦角醇","info_province_id":23,"info_payment_way":"3501","info_refundable_time":1747211814000,"info_order_status":"1001","info_create_time":1746607014000,"id":304,"spu_id":3,"table":"comment_info","info_tm_ms":1746671516223,"op":"c","create_time":1746607062000,"info_user_id":301,"info_op":"c","info_trade_body":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机等3件商品","sku_id":10,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13933927978","info_total_amount":"16463.00","info_out_trade_no":"312468523222182","appraise":"1201","user_id":301,"info_id":2816,"info_coupon_reduce_amount":"0.00","order_id":2816,"info_consignee":"柳姬","ts_ms":1746671516667,"db":"gmall_config"}

        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        }).uid("add json ds").name("add json ds");
//        suppleTimeFieldDs.print();
//        change commentTxt: {"info_original_total_amount":"22597.00","info_activity_reduce_amount":"1339.80","commentTxt":"TCL 75Q10电视画质一般，音响效果差，操作界面复杂，不推荐购买。,裸照图片","info_province_id":20,"info_payment_way":"3501","info_refundable_time":1747263055000,"info_order_status":"1004","info_create_time":1746658255000,"id":419,"spu_id":5,"table":"comment_info","info_operate_time":1746658300000,"info_tm_ms":1746671649756,"op":"c","create_time":1746658300000,"info_user_id":919,"info_op":"u","info_trade_body":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等3件商品","sku_id":18,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13516824833","info_total_amount":"21257.20","info_out_trade_no":"698419251887457","appraise":"1202","user_id":919,"info_id":3892,"info_coupon_reduce_amount":"0.00","order_id":3892,"info_consignee":"钱泽晨","ts_ms":1746671649745,"db":"gmall_config"}
//        16> {"info_original_total_amount":"22597.00","info_activity_reduce_amount":"1339.80","commentTxt":"TCL 75Q10电视画质一般，音响效果差，操作界面复杂，不推荐购买。,裸照图片","info_province_id":20,"info_payment_way":"3501","ds":"20250508","info_refundable_time":1747263055000,"info_order_status":"1004","info_create_time":1746658255000,"id":419,"spu_id":5,"table":"comment_info","info_operate_time":1746658300000,"info_tm_ms":1746671649756,"op":"c","create_time":1746658300000,"info_user_id":919,"info_op":"u","info_trade_body":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等3件商品","sku_id":18,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13516824833","info_total_amount":"21257.20","info_out_trade_no":"698419251887457","appraise":"1202","user_id":919,"info_id":3892,"info_coupon_reduce_amount":"0.00","order_id":3892,"info_consignee":"钱泽晨","ts_ms":1746671649745,"db":"gmall_config"}
        suppleTimeFieldDs.map(js -> js.toJSONString())
                .sinkTo(
                KafkaUtils.buildKafkaSink("cdh01:9092","db_fact_comment_topic")
        ).uid("kafka_db_fact_comment_sink").name("kafka_db_fact_comment_sink");


        env.execute();

    }
}
