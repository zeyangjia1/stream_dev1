package DWD;

import Base.BaseApp;
import Bean.DimBaseCategory;
import Constant.Constant;
import DWD.func.*;
import Utils.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import func.FilterBloomDeduplicatorUidFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
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
        new AgeThemeSummaryDetails().start(9090, 2, "AgeThemeSummaryDetails", Constant.topic_db);
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
            // 执行查询并缓存分类数据（避免重复查询数据库）
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
        //读取业务总数据kafka
//        kafkaStrDS.print();
//        2> {"before":{"id":698,"consignee":"钟离有坚","consignee_tel":"13821993433","total_amount":"3927.00","order_status":"1001","user_id":213,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"392427196812226","trade_body":"华为智慧屏V55i-J 55英寸 HEGE-550B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 银钻灰 京品家电等1件商品","create_time":1747344456000,"operate_time":null,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":29,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"3927.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747949256000},"after":{"id":698,"consignee":"钟离有坚","consignee_tel":"13821993433","total_amount":"3927.00","order_status":"1002","user_id":213,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"392427196812226","trade_body":"华为智慧屏V55i-J 55英寸 HEGE-550B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 银钻灰 京品家电等1件商品","create_time":1747344456000,"operate_time":1747344488000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":29,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"3927.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747949256000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747295629000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_info","server_id":1,"gtid":null,"file":"mysql-bin.000053","pos":7654614,"row":0,"thread":181,"query":null},"op":"u","ts_ms":1747295627519,"transaction":null}
        //读取六大标签主体数据
//        dmp_user.print();
//        1> {"birthday":"1970-05-15","uname":"华壮会","gender":"M","weight":"53","uid":"97","login_name":"jtb18dw2","unit_height":"cm","ear":1970,"user_level":"1","phone_num":"13192663935","unit_weight":"kg","email":"jtb18dw2@3721.net","ts_ms":1747295975835,"height":"160"}

        // 过滤 明细表
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_detail"));
//        order_detail.print();
//        2> {"op":"c","after":{"sku_num":1,"create_time":1747342416000,"split_coupon_amount":"0.00","sku_id":32,"sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 5号淡香水35ml","order_price":"300.00","id":1281,"order_id":706,"split_activity_amount":"0.00","split_total_amount":"300.00"},"source":{"thread":194,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000053","connector":"mysql","pos":7647523,"name":"mysql_binlog_source","row":0,"ts_ms":1747295629000,"snapshot":"false","db":"realtime_v2","table":"order_detail"},"ts_ms":1747295627481}

        //过滤订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_info"));
//        order_info.print();
//        2> {"op":"u","before":{"payment_way":"3501","refundable_time":1747946074000,"original_total_amount":"20325.00","order_status":"1002","consignee_tel":"13438775621","trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3070Ti 钛晶灰等3件商品","id":701,"operate_time":1747341317000,"consignee":"窦辰士","create_time":1747341274000,"coupon_reduce_amount":"0.00","out_trade_no":"558255695953769","total_amount":"20075.00","user_id":226,"province_id":16,"activity_reduce_amount":"250.00"},"after":{"payment_way":"3501","refundable_time":1747946074000,"original_total_amount":"20325.00","order_status":"1004","consignee_tel":"13438775621","trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3070Ti 钛晶灰等3件商品","id":701,"operate_time":1747341348000,"consignee":"窦辰士","create_time":1747341274000,"coupon_reduce_amount":"0.00","out_trade_no":"558255695953769","total_amount":"20075.00","user_id":226,"province_id":16,"activity_reduce_amount":"250.00"},"source":{"thread":195,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000053","connector":"mysql","pos":7675304,"name":"mysql_binlog_source","row":0,"ts_ms":1747295629000,"snapshot":"false","db":"realtime_v2","table":"order_info"},"ts_ms":1747295627697}

        //读取日志数据
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
//        kafkaDs.print();
//        2> {"common":{"ar":"16","ba":"realme","ch":"xiaomi","is_new":"1","md":"realme Neo2","mid":"mid_358","os":"Android 13.0","sid":"e104eb56-266e-4d9c-8493-931f349ef50b","uid":"493","vc":"v2.1.132"},"displays":[{"item":"11","item_type":"sku_id","pos_id":4,"pos_seq":0},{"item":"31","item_type":"sku_id","pos_id":4,"pos_seq":1},{"item":"33","item_type":"sku_id","pos_id":4,"pos_seq":2},{"item":"15","item_type":"sku_id","pos_id":4,"pos_seq":3},{"item":"28","item_type":"sku_id","pos_id":4,"pos_seq":4},{"item":"7","item_type":"sku_id","pos_id":4,"pos_seq":5},{"item":"6","item_type":"sku_id","pos_id":4,"pos_seq":6},{"item":"15","item_type":"sku_id","pos_id":4,"pos_seq":7},{"item":"1","item_type":"sku_id","pos_id":4,"pos_seq":8}],"page":{"during_time":16541,"item":"3","item_type":"sku_id","last_page_id":"register","page_id":"good_detail"},"ts":1747322158082}
        //解析
        SingleOutputStreamOperator<JSONObject> map1 = kafkaDs.map(JSONObject::parseObject);
        //提取 搜索词 和 os
        SingleOutputStreamOperator<JSONObject> pageLog = map1.map(new PageOsUid());
//        pageLog.print();
//        2> {"uid":"720","deviceInfo":{"ar":"24","uid":"720","os":"Android","ch":"oppo","md":"realme Neo2","vc":"v2.1.134","ba":"realme"},"ts":1747322936010}

        //计算  年代 年龄 星座 主题数据
        SingleOutputStreamOperator<JSONObject> dmp_user_age = dmp_user.map(JSONObject::parseObject).process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String birthday = jsonObject.getString("birthday");
                String age = AgeGroupFunc.getAgeRange(birthday);
                String era = TimePeriodFunc.getEra(birthday);
                String constellation = TimePeriodFunc.getConstellation(birthday);
                JSONObject jsonObject1 = new JSONObject();
                jsonObject1.putAll(jsonObject);
                jsonObject1.put("age_group", age);
                jsonObject1.put("era", era);
                jsonObject1.put("constellation", constellation);
                collector.collect(jsonObject1);
            }
        });
//        dmp_user_age.print();
//        1> {"birthday":"2002-11-08","uname":"黄振壮","age_group":"18-24","weight":"51","uid":"21","login_name":"hjhqtwl2","unit_height":"cm","constellation":"天蝎座","era":"00年代","ear":2000,"user_level":"1","phone_num":"13889387431","unit_weight":"kg","email":"hjhqtwl2@126.com","ts_ms":1747295931694,"height":"152"}
        //使用布隆按照uid 和 ts 过滤
        SingleOutputStreamOperator<JSONObject> BloomLog = pageLog.keyBy(o -> o.getString("uid"))
                .filter(new FilterBloomDeduplicatorUidFunc(1000000, 0.0001, "uid", "ts"));
//        BloomLog.print();
//        2> {"uid":"357","deviceInfo":{"ar":"18","uid":"357","os":"Android","ch":"xiaomi","md":"realme Neo2","vc":"v2.1.134","ba":"realme"},"ts":1747312317302}
        // 3 分钟窗口 处理uid 的设备信息 合并  等
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = BloomLog.keyBy(o -> o.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
                .reduce((value1, value2) -> value2);
//        win2MinutesPageLogsDs.print();

        //主标签和搜索设备关联
        SingleOutputStreamOperator<JSONObject> dmp_user_age_search = dmp_user_age.keyBy(o -> o.getString("uid"))
                .intervalJoin(win2MinutesPageLogsDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.putAll(a);
                        aa.remove("ts");
                        jsonObject.putAll(aa);
                        collector.collect(jsonObject);
                    }
                });
//        dmp_user_age_search.print();
//        {"birthday":"1968-02-08","uname":"方晶妍","gender":"F","os":"iOS,Android","ch":"Appstore,oppo,wandoujia,360","age_group":"50岁以上","pv":42,"weight":"58","uid":"50","login_name":"ok3hqncd3wx","unit_height":"cm","ear":1960,"md":"iPhone 14,realme Neo2,iPhone 13,iPhone 14 Plus,vivo IQOO Z6x ,SAMSUNG Galaxy S21,vivo x90","user_level":"1","phone_num":"13199299967","unit_weight":"kg","search_item":"联想,衬衫","email":"ok3hqncd3wx@0355.net","ts_ms":1747295911194,"height":"161","ts":1747265934134,"ba":"iPhone,realme,vivo,SAMSUNG"}


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
//        uid_order_time.print();
//        1> {"birthday":"2001-03-08","create_time":"1747343764000","uname":"南门勤","gender":"F","age_group":"18-24","weight":"40","uid":"4","login_name":"7bgplo7da6h2","unit_height":"cm","constellation":"双鱼座","total_amount":"69.00","era":"00年代","ear":2000,"user_level":"1","phone_num":"13418647143","unit_weight":"kg","order_id":"695","email":"7bgplo7da6h2@163.com","ts_ms":1747295955916,"height":"155"}

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

//        uid_order_time_related_priceRange.print();
//        1> {"birthday":"2005-06-15","create_time":"1747344548000","uname":"苗涛","gender":"M","age_group":"18-24","weight":"61","uid":"230","login_name":"r8bnek","unit_height":"cm","constellation":"双子座","total_amount":"20397.00","era":"00年代","ear":2000,"user_level":"1","phone_num":"13874647365","unit_weight":"kg","order_id":"714","priceRange":"高价商品","email":"r8bnek@3721.net","ts_ms":1747295627308,"time_period":"早晨","height":"152"}

        //金额 时间打分
        SingleOutputStreamOperator<JSONObject> time_related_priceRange = uid_order_time_related_priceRange.map(new PriceTime());
//        time_related_priceRange.print();
//        2> {"birthday":"1995-11-08","create_time":"1747293338000","uname":"俞宜","gender":"F","age_group":"25-29","weight":"50","uid":"38","login_name":"745p7p2rrfe","unit_height":"cm","constellation":"天蝎座","total_amount":"17499.00","era":"90年代","ear":1990,"user_level":"1","phone_num":"13987866129","unit_weight":"kg","order_id":"583","priceRange":"高价商品","email":"745p7p2rrfe@aol.com","ts_ms":1747295911172,"time_period":"下午","height":"173","result1":{"te_25-29":0.05,"tl_25-29":0.03}}


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
//        order_info_detail.print();
//        1> {"uid":"39","sku_id":"31"}

        //关联 sku 取 三级 品类 id 和 品牌 id异步
        SingleOutputStreamOperator<JSONObject> order_detail_sku = order_info_detail.map(new OrderDetailSkuHbase());
//        order_detail_sku.print();
//        1> {"uid":"209","tm_id":"5","category3_id":"61"}

        //关联 三级品类异步
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category3 = order_detail_sku.map(new order_detail_sku_category3Func());
//        order_detail_sku_category3.print();
//        1> {"uid":"206","tm_id":"3","category3_name":"游戏本","category3_id":"287","category2_id":"33"}

        //关联 二级品类异步
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category2 = order_detail_sku_category3.map(new order_detail_sku_category2Func());
//        order_detail_sku_category2.print();
//        1> {"category1_id":"8","uid":"209","category2_name":"香水彩妆","tm_id":"11","category3_name":"香水","category3_id":"473","category2_id":"54"}
        //关联  一级 品类异步
        SingleOutputStreamOperator<JSONObject> order_detail_sku_category = order_detail_sku_category2.map(new order_detail_sku_categoryFunc());
//        order_detail_sku_category.print();
//        2> {"category1_id":"2","uid":"38","category2_name":"手机通讯","tm_id":"2","category1_name":"手机","category3_name":"手机","category3_id":"61","category2_id":"13"}
        //汇总类目字段
        SingleOutputStreamOperator<JSONObject> category = order_detail_sku_category.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String category2_name = jsonObject.getString("category2_name");
                String category1_name = jsonObject.getString("category1_name");
                String category3_name = jsonObject.getString("category3_name");
                String uid = jsonObject.getString("uid");
                JSONObject a = new JSONObject();
                a.put("category_name", category3_name);
                a.put("category_name", category2_name);
                a.put("category_name", category1_name);
                a.put("uid", uid);
                collector.collect(a);

            }
        });
//        category.print();
//        2> {"uid":"34","category_name":"手机"}
        //主标签和品类关联
        SingleOutputStreamOperator<JSONObject> user_category = dmp_user_age.keyBy(o -> o.getString("uid"))
                .intervalJoin(category.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.putAll(a);
                        jsonObject.put("category_name", aa.getString("category_name"));
                        collector.collect(jsonObject);
                    }
                });
//        user_category.print();
//        1> {"birthday":"1996-01-08","category_name":"个护化妆","uname":"许林有","age_group":"25-29","weight":"55","uid":"39","login_name":"xejhlltccc","unit_height":"cm","constellation":"摩羯座","era":"90年代","ear":1990,"user_level":"1","phone_num":"13379219833","unit_weight":"kg","email":"xejhlltccc@sohu.com","ts_ms":1747295917856,"height":"167"}

        //关联 品牌表异步
        SingleOutputStreamOperator<JSONObject> order_detail_sku_tm = order_detail_sku.map(new order_detail_sku_tm_func());
//        order_detail_sku_tm.print();
//        1> {"tm_name":"Redmi","uid":"38","tm_id":"1"}
//    主标签和品类关联 后关联 品牌
        SingleOutputStreamOperator<JSONObject> user_category_tm = user_category.keyBy(o -> o.getString("uid"))
                .intervalJoin(order_detail_sku_tm.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject a, JSONObject aa, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                            jsonObject.putAll(a);
                            jsonObject.put("tm_name", aa.getString("tm_name"));
                            collector.collect(jsonObject);
                    }
                });
//        user_category_tm.print();
//        2> {"birthday":"1995-11-08","category_name":"手机","uname":"俞宜","gender":"F","age_group":"25-29","weight":"45","tm_name":"长粒香","uid":"38","login_name":"745p7p2rrfe","unit_height":"cm","constellation":"天蝎座","era":"90年代","ear":1990,"user_level":"1","phone_num":"13987866129","unit_weight":"kg","email":"745p7p2rrfe@aol.com","ts_ms":1747295911172,"height":"166"}
        //过滤 品牌类目 去除null

        SingleOutputStreamOperator<JSONObject> user_category_tm_fifter = user_category_tm.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                if (jsonObject.containsKey("category_name")
                        && jsonObject.containsKey("tm_name")
                        && !jsonObject.getString("category_name").isEmpty()
                        && !jsonObject.getString("tm_name").isEmpty()) {
                    collector.collect(jsonObject);
                }

            }
        });
//        2> {"birthday":"1995-11-08","category_name":"手机","uname":"俞宜","gender":"F","age_group":"25-29","weight":"45","tm_name":"长粒香","uid":"38","login_name":"745p7p2rrfe","unit_height":"cm","constellation":"天蝎座","era":"90年代","ear":1990,"user_level":"1","phone_num":"13987866129","unit_weight":"kg","email":"745p7p2rrfe@aol.com","ts_ms":1747295911172,"height":"166"}


        //金额时间计算权重后去重
        SingleOutputStreamOperator<JSONObject> BloomLTimeTotal = time_related_priceRange.keyBy(o -> o.getString("uid"))
                .filter(new FilterBloomDeduplicatorUidFunc(1000000, 0.0001, "uid", "ts_ms"));
//        BloomLTimeTotal:2> {"birthday":"1982-03-08","create_time":"1747339421000","uname":"伍婉娴","age_group":"40-49","weight":"71","uid":"18","login_name":"x2egtg6p8","unit_height":"cm","total_amount":"3299.00","ear":1980,"user_level":"2","phone_num":"13597716793","unit_weight":"kg","order_id":"666","priceRange":"中间商品","email":"x2egtg6p8@3721.net","ts_ms":1747295931689,"time_period":"凌晨","height":"158","result1":{"te_40-49":0.01}}
//        BloomLTimeTotal.print("BloomLTimeTotal");

        //cm 是类目 tm 是品牌 计算权重
        SingleOutputStreamOperator<JSONObject> CmTmScore = user_category_tm_fifter.map(new CategoryTmName());
//        CmTmScore.print("CmTmScore");
        //        4> {"birthday":"1995-11-08","category_name":"手机","tm_source":{"tm_25_29":0.06},"uname":"俞宜","gender":"F","age_group":"25-29岁","cm_source":{"cm_25_29":0.12},"weight":"40","tm_name":"香奈儿","uid":"38","login_name":"745p7p2rrfe","unit_height":"cm","ear":1990,"user_level":"1","phone_num":"13987866129","unit_weight":"kg","email":"745p7p2rrfe@aol.com","ts_ms":1747295911172,"height":"164"}

        //调用打分模型  设备    搜索词
        SingleOutputStreamOperator<JSONObject> search_device_source = dmp_user_age_search.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        search_device_source.print();
//        1> {"birthday":"1974-08-08","device_25-29":0.06,"pv":48,"device_40-49":0.03,"search_30-34":0,"search_18-24":0,"uid":"27","unit_height":"cm","constellation":"狮子座","era":"70年代","ear":1970,"md":"iPhone 14 Plus,OPPO Remo8,Redmi k50,vivo x90,xiaomi 12 ultra ","phone_num":"13763784866","search_item":"","search_25-29":0,"email":"2r5r9nx5cm@aol.com","device_30-34":0.05,"search_40-49":0,"height":"187","device_18-24":0.07,"uname":"于盛雄","os":"iOS,Android","device_50":0.02,"ch":"Appstore,xiaomi,oppo,web,wandoujia,vivo","age_group":"50","weight":"68","search_35-39":0,"search_50":0,"device_35-39":0.04,"login_name":"2r5r9nx5cm","user_level":"1","judge_os":"iOS","unit_weight":"kg","ts_ms":1747295944950,"ba":"iPhone,xiaomi,OPPO,vivo,Redmi"}

        //设备 搜索词 打分后 计算
// 计算特定年龄段在设备和搜索维度上的数值总和，并添加到JSON对象中
        SingleOutputStreamOperator<JSONObject> search_device_source_tmp = search_device_source.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 需要汇总的字段前缀
                String[] fields2 = {"device", "search"};
                // 从JSON中获取年龄段信息（如"age_18_25"）
                String ageGroup = jsonObject.getString("age_group");
                // 初始化总和为零，使用BigDecimal保证精度
                BigDecimal sum = BigDecimal.ZERO;
                // 遍历设备和搜索字段，累加对应年龄段的值
                for (String s : fields2) {
                    // 构建完整字段名
                    String fieldName = s + "_" + ageGroup;
                    // 从JSON中获取BigDecimal类型的值，不存在的话使用零值
                    BigDecimal fieldValue = jsonObject.getBigDecimal(fieldName);
                    //如何不是空的就加入总和,是null 就加0 避免 空指针
                    sum = sum.add(fieldValue != null ? fieldValue : BigDecimal.ZERO);
                }
                jsonObject.put("sum", sum);
                // 保留三位小数
                sum = sum.setScale(3, RoundingMode.HALF_UP);
                // 转换为double类型
                double aDouble = sum.doubleValue();
                // 返回处理后的JSON对象
                return jsonObject;
            }
        });

//        search_device_source_tmp.print("tmp-->");
//        {"birthday":"2007-03-15","device_25-29":0.06,"pv":59,"device_40-49":0.03,"search_30-34":0,"sum":0.07,"search_18-24":0,"uid":"76","unit_height":"cm","constellation":"双鱼座","era":"00年代","ear":2000,"md":"iPhone 14,xiaomi 13,realme Neo2,iPhone 13,iPhone 14 Plus,xiaomi 13 Pro ,Redmi k50,xiaomi 12 ultra ","phone_num":"13811637756","search_item":"联想,iPhone14","search_25-29":0,"email":"jin0w0kfa@yahoo.com.cn","device_30-34":0.05,"search_40-49":0,"height":"170","device_18-24":0.07,"uname":"戴静淑","os":"iOS,Android","device_50":0.02,"ch":"Appstore,xiaomi,oppo,web,vivo,360","age_group":"18-24","weight":"76","search_35-39":0,"search_50":0,"device_35-39":0.04,"login_name":"jin0w0kfa","user_level":"2","judge_os":"iOS","unit_weight":"kg","ts_ms":1747295972435,"ba":"iPhone,xiaomi,realme,Redmi"}

        //六个标签写入到kafka
        SingleOutputStreamOperator<String> price_string = BloomLTimeTotal.map(JSON::toString);
        SingleOutputStreamOperator<String> cmtm_string = CmTmScore.map(JSON::toString);
        SingleOutputStreamOperator<String> search_string = search_device_source_tmp.map(JSON::toString);
        KafkaSink<String> price_string1 = FinkSink.getkafkasink("TotalSource_string_v2");
        KafkaSink<String> cmtm_string1 = FinkSink.getkafkasink("CmTmSource_string_v2");
        KafkaSink<String> search_string1 = FinkSink.getkafkasink("SearchSource_string_v2");
//        price_string.sinkTo(price_string1);
//        cmtm_string.sinkTo(cmtm_string1);
//        search_string.sinkTo(search_string1);



    }
}
