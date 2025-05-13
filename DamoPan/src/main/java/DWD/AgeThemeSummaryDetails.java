package DWD;
import Base.BaseApp;
import Constant.Constant;
import Utils.FlinkSource;
import Utils.Hbaseutlis;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Duration;
/**
 * @Package DWD.AgeThemeSummaryDetails
 * @Author zeyang_jia
 * @Date 2025/5/12 15:08
 * @description: 年龄主题字段汇总
 */
public class AgeThemeSummaryDetails extends BaseApp {
    public static void main(String[] args) throws Exception {
        new AgeThemeSummaryDetails().start(9090, 4, "topic_db", Constant.topic_db);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 读取标签数据
        KafkaSource<String> kafkaSource1 = FlinkSource.getKafkaSource(Constant.dmp_user_info);
        DataStreamSource<String> dmp_user = env.fromSource(kafkaSource1,
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
                        ), "kafka_source");
//        kafkaStrDS.print();
        // 过滤 明细表价格 order_detail
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_detail")
        );
        //过滤订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_info"));


        KafkaSource<String> kafkaSource = FlinkSource.getKafkaSource(Constant.topic_log);
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        SingleOutputStreamOperator<JSONObject> map1 = kafkaDs.map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> pageLog = map1.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                                        JSONObject object = new JSONObject();
                if (jsonObject.containsKey("common")){
                    JSONObject common = jsonObject.getJSONObject("common");
                    object.put("uid",common.getString("uid"));
                    object.put("ts",jsonObject.getLong("ts"));
                    // 去掉版本直接获取 OS
                    String os = common.getString("os").split(" ")[0];
                    object.put("os",os);
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                        JSONObject page = jsonObject.getJSONObject("page");
                        if (page.containsKey("item_type") && page.getString("item_type").equals("keyword")){
                            object.put("keyword",page.getString("item"));
                        }
                    }
                }
        )


//        RichMapFunction<JSONObject,JSONObject>{
//
//            @Override
//            public JSONObject map(JSONObject jsonObject){

//                return object;
//            }

//        订单->明细->sku->品牌->三级品类->二级品类->一级品类->用户-1> 日志
        KeyedStream<JSONObject, String> uid_key = dmp_user.map(JSON::parseObject)
                .keyBy(o -> o.getString("uid"));
        KeyedStream<JSONObject, String> order_key = order_info.filter(o -> !o.getJSONObject("after").getString("user_id").isEmpty())
                .keyBy(o -> o.getJSONObject("after").getString("user_id"));

        //关联 订单表时间 字段
        SingleOutputStreamOperator<JSONObject> uid_order_time = order_key
                .intervalJoin(uid_key)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, Context context, Collector<JSONObject> collector) throws Exception {
                        if (a != null && a.getJSONObject("after") != null && a.getJSONObject("after").getString("user_id") != null && a.getJSONObject("after").getString("create_time")!=null) {
                            String userId = a.getJSONObject("after").getString("user_id");
                            String uid = aa.getString("uid");
                            if (userId.equals(uid)) {
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("create_time", a.getJSONObject("after").getString("create_time"));
                                jsonObject.put("order_id", a.getJSONObject("after").getString("id"));

                                jsonObject.putAll(aa);
                                collector.collect(jsonObject);
                            }
                        }
                    }
                });



        KeyedStream<JSONObject, String> order_detail_key = order_detail.keyBy(o -> o.getJSONObject("after").getString("order_id"));
        KeyedStream<JSONObject, String> order_uid_after_key = uid_order_time.keyBy(o -> o.getString("order_id"));
        //关联 ￥ 字段
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount = order_uid_after_key
                .intervalJoin(order_detail_key)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.putAll(a);
                        jsonObject.put("split_total_amount", aa.getJSONObject("after").getString("split_total_amount"));
                        jsonObject.put("sku_id", aa.getJSONObject("after").getString("sku_id"));

                        collector.collect(jsonObject);

                    }
                });
        //关联 sku 取 tm_id
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount_sku = order_uid_total_amount.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("sku_id");
                        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_sku_info", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("tm_id", skuInfoJsonObj.getString("tm_id"));
                                a.put("category3_id", skuInfoJsonObj.getString("category3_id"));

                        return a;
                    }
                }
        );

            //关联 品牌
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount_sku_trademark = order_uid_total_amount_sku.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String tm_id = jsonObject.getString("tm_id");
                        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_trademark", tm_id, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("tm_name", skuInfoJsonObj.getString("tm_name"));

                        return a;
                    }
                }
        );
        //关联 三级品类
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount_sku_trademark_category3 = order_uid_total_amount_sku_trademark.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category3_id");
                        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_category3", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category3_name", skuInfoJsonObj.getString("name"));
                        a.put("category2_id", skuInfoJsonObj.getString("category2_id"));

                        return a;
                    }
                }
        );


        //关联 二级品类
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount_sku_trademark_category3_category2 = order_uid_total_amount_sku_trademark_category3.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category2_id");
                        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_category2", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category2_name", skuInfoJsonObj.getString("name"));
                        a.put("category1_id", skuInfoJsonObj.getString("category1_id"));

                        return a;
                    }
                }
        );

        //关联 一级品类
        SingleOutputStreamOperator<JSONObject> order_uid_total_amount_sku_trademark_category3_category2_category1 = order_uid_total_amount_sku_trademark_category3_category2.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category1_id");
                        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_category1", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category1_name", skuInfoJsonObj.getString("name"));

                        return a;
                    }
                }
        );



    }
}

