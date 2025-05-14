package DWD;

import Base.BaseApp;
import Bean.DimBaseCategory;
import Constant.Constant;
import DWD.func.*;
import Utils.ConfigUtils;
import Utils.FlinkSource;
import Utils.Hbaseutlis;
import Utils.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import func.FilterBloomDeduplicatorUidFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;

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
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                   "jdbc:mysql://cdh03:3306/realtime_v2?useSSL=false",
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v2.base_category3 as b3  \n" +
                    "     join realtime_v2.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v2.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 读取标签数据
        KafkaSource<String> kafkaSource1 = FlinkSource.getKafkaSource(Constant.dmp_user_info);
        DataStreamSource<String> dmp_user = env.fromSource(kafkaSource1,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null) {
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ), "kafka_source");
//        kafkaStrDS.print();
        // 过滤 明细表
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_detail"));
        //过滤订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_info"));

        //读取page
        KafkaSource<String> kafkaSource = FlinkSource.getKafkaSource(Constant.topic_log);
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }
                ), "kafka_source");

        SingleOutputStreamOperator<JSONObject> map1 = kafkaDs.map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> pageLog = map1.map(new PageOsUid());

        //计算  年龄  六大标签
        SingleOutputStreamOperator<JSONObject> dmp_user_age = dmp_user.map(JSONObject::parseObject).process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String birthday = jsonObject.getString("birthday");
                String age = AgeGroupFunc.getAgeRange(birthday);
                JSONObject jsonObject1 = new JSONObject();
                jsonObject1.putAll(jsonObject);
                jsonObject1.put("age_group", age);
                collector.collect(jsonObject1);
            }
        });



        //使用布隆按照uid 和 ts 过滤
        SingleOutputStreamOperator<JSONObject> BloomLog = pageLog.keyBy(o -> o.getString("uid"))
                .filter(new FilterBloomDeduplicatorUidFunc(1000000, 0.0001, "uid", "ts"));

        // 3 分钟窗口 处理uid 的设备信息 合并  等
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = BloomLog.keyBy(o -> o.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
                .reduce((value1, value2) -> value2);






        //调用打分模型进行打分
        win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories,device_rate_weight_coefficient,search_rate_weight_coefficient))
                .print();
//        1> {"device_35_39":0.04,"os":"iOS,Android","device_50":0.02,"search_25_29":0,"ch":"Appstore,xiaomi,oppo,wandoujia","pv":90,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"2","device_25_29":0.06,"md":"iPhone 14,xiaomi 13,iPhone 13,iPhone 14 Plus,vivo IQOO Z6x ,OPPO Remo8,vivo x90,Redmi k50,xiaomi 12 ultra ","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"iPhone14,小米","ba":"iPhone,xiaomi,OPPO,vivo,Redmi","search_30_34":0}


        KeyedStream<JSONObject, String> dmp_uid_key = dmp_user_age
                .keyBy(o -> o.getString("uid"));
        KeyedStream<JSONObject, String> order_key = order_info.filter(o -> !o.getJSONObject("after").getString("user_id").isEmpty())
                .keyBy(o -> o.getJSONObject("after").getString("user_id"));

        //关联 订单表时间 和   总金额  字段
        SingleOutputStreamOperator<JSONObject> uid_order_time = order_key
                .intervalJoin(dmp_uid_key)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, Context context, Collector<JSONObject> collector) throws Exception {
                        if (a != null && a.getJSONObject("after") != null && a.getJSONObject("after").getString("user_id") != null && a.getJSONObject("after").getString("create_time") != null) {
                            String userId = a.getJSONObject("after").getString("user_id");
                            String uid = aa.getString("uid");
                            if (userId.equals(uid)) {
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("create_time", a.getJSONObject("after").getString("create_time"));
                                jsonObject.put("total_amount", a.getJSONObject("after").getString("total_amount"));
                                jsonObject.put("order_id", a.getJSONObject("after").getString("id"));
                                jsonObject.putAll(aa);
                                collector.collect(jsonObject);
                            }
                        }
                    }
                });

        //计算 时间和金额 行为
        SingleOutputStreamOperator<JSONObject> uid_order_time_related_priceRange = uid_order_time.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                Long create_time = jsonObject.getLong("create_time");
                Double total_amount = jsonObject.getDouble("total_amount");
                String timePeriod = TimePeriodFunc.getTimePeriod(create_time);
                String priceRange = TimePeriodFunc.getPriceRange(total_amount);
                jsonObject.put("time_period", timePeriod);
                jsonObject.put("priceRange", priceRange);
                collector.collect(jsonObject);
            }
        });
        uid_order_time_related_priceRange.print();

//        uid_order_time_related_priceRange.filter(o->!o.getString("age_group").equals("未知")).print();


        //关联 订单 和明细 提取 sku 和 uid
        KeyedStream<JSONObject, String> order_info_key = order_info.keyBy(o -> o.getJSONObject("after").getString("id"));
        KeyedStream<JSONObject, String> order_detail_key = order_detail.keyBy(o -> o.getJSONObject("after").getString("order_id"));
        SingleOutputStreamOperator<JSONObject> order_info_detail = order_info_key.intervalJoin(order_detail_key)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("uid", a.getJSONObject("after").getString("user_id"));
                        result.put("sku_id", aa.getJSONObject("after").getString("sku_id"));
                        collector.collect(result);
                    }


                });

        //关联 sku 取 三级 品类 id 和 品牌 id
        SingleOutputStreamOperator<JSONObject> order_detail_sku = order_info_detail.map(new OrderDetailSkuHbase());
        //关联 三级品类
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category3 = order_detail_sku.map(new order_detail_sku_category3Func());
        //关联 二级品类
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category2 = order_detail_sku_category3.map(new order_detail_sku_category2Func());
        //关联  一级 品类
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category = order_detail_sku_category2.map(new order_detail_sku_categoryFunc());
        //关联 品牌表
        SingleOutputStreamOperator<JSONObject> order_detail_sku_tm = order_detail_sku.map(new order_detail_sku_tm_func());

    }
}
