package DWD;

import Base.BaseApp;
import Constant.Constant;
import Utils.FlinkSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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
        SingleOutputStreamOperator<JSONObject> user_info_fifter = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("user_info"));
        //处理用户表生日字段
        SingleOutputStreamOperator<JSONObject> result_user = user_info_fifter.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 从输入的 JSON 对象中获取 "after" 字段，该字段通常包含数据库变更后的数据
                JSONObject afterObj = jsonObject.getJSONObject("after");

                // 如果 afterObj 不为空且包含 birthday 字段，则进行处理
                if (afterObj != null && afterObj.containsKey("birthday")) {
                    // 获取 birthday 字段的值，可能是字符串或数字形式的出生日期
                    Object birthdayValue = afterObj.get("birthday");

                    // 如果 birthdayValue 不为 null，则尝试将其转换为标准日期格式
                    if (birthdayValue != null) {
                        // 将 birthday 转换为字符串形式以便后续解析
                        String daysSinceBaseStr = birthdayValue.toString();

                        try {
                            // 尝试将字符串解析为表示自 1970-01-01 起的天数的 long 值
                            long days = Long.parseLong(daysSinceBaseStr);

                            // 定义基准日期（Unix 时间起点）
                            LocalDate baseDate = LocalDate.of(1970, 1, 1);

                            // 计算实际出生日期
                            LocalDate birthDate = baseDate.plusDays(days);

                            // 使用 ISO 标准格式化日期（如：2023-04-05）
                            String formattedDate = birthDate.format(DateTimeFormatter.ISO_LOCAL_DATE);

                            // 将原始的 birthday 字段替换为格式化后的日期字符串
                            afterObj.put("birthday", formattedDate);
                        } catch (NumberFormatException e) {
                            // 如果解析失败（例如字段不是有效的数字），则标记生日为无效
                            afterObj.put("birthday", "invalid_date");
                        }
                    }
                }

                // 返回处理后的 JSON 对象
                return jsonObject;
            }
        });
//        result.print("处理完用户表生日字段 ");
//        4> {"op":"c","after":{"birthday":"1996-10-12","gender":"F","create_time":1747087262000,"login_name":"xdxvmb3kf","nick_name":"倩婷","name":"曹倩婷","user_level":"2","phone_num":"13838356998","id":89,"email":"xdxvmb3kf@googlemail.com"},"source":{"thread":1301,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":63126213,"name":"mysql_binlog_source","row":0,"ts_ms":1747030156000,"snapshot":"false","db":"realtime_v2","table":"user_info"},"ts_ms":1747030155352}
        //过滤 一级  类目
        SingleOutputStreamOperator<JSONObject> base_category1 = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category1"));
//        base_category1.print();
//        4> {"op":"c","after":{"create_time":1639440000000,"name":"图书、音像、电子书刊","id":1},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60219222,"name":"mysql_binlog_source","row":0,"ts_ms":1747030130000,"snapshot":"false","db":"realtime_v2","table":"base_category1"},"ts_ms":1747030128667}

        //过滤二级 类目
        SingleOutputStreamOperator<JSONObject> base_category2 = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category2"));
//        base_category2.print();
//        4> {"op":"c","after":{"category1_id":12,"create_time":1639440000000,"name":"妈妈专区","id":70},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60247257,"name":"mysql_binlog_source","row":0,"ts_ms":1747030130000,"snapshot":"false","db":"realtime_v2","table":"base_category2"},"ts_ms":1747030128846}

        //过滤三级 类目

        SingleOutputStreamOperator<JSONObject> base_category3 = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category3"));
//        base_category3.print();
//        4> {"op":"c","after":{"create_time":1639440000000,"name":"轮滑滑板","id":1098,"category2_id":113},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60609928,"name":"mysql_binlog_source","row":0,"ts_ms":1747030132000,"snapshot":"false","db":"realtime_v2","table":"base_category3"},"ts_ms":1747030131210}
        // 过滤 品牌 表
        SingleOutputStreamOperator<JSONObject> base_trademark = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_trademark"));
//        base_trademark.print();
//        4> {"op":"c","after":{"tm_name":"Redmi","create_time":1639440000000,"id":1},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60652909,"name":"mysql_binlog_source","row":0,"ts_ms":1747030132000,"snapshot":"false","db":"realtime_v2","table":"base_trademark"},"ts_ms":1747030131519}
        // 过滤 明细表价格 order_detail
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_detail"));
//        order_detail.print();
//        4> {"op":"c","after":{"sku_num":1,"create_time":1747092625000,"split_coupon_amount":"0.00","sku_id":11,"sku_name":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机","order_price":"8197.00","id":320,"order_id":190,"split_activity_amount":"0.00","split_total_amount":"8197.00"},"source":{"thread":1308,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":63563230,"name":"mysql_binlog_source","row":0,"ts_ms":1747030158000,"snapshot":"false","db":"realtime_v2","table":"order_detail"},"ts_ms":1747030157375}
        //过滤订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_info"));
//        order_info.print();
//        4> {"op":"c","after":{"category1_id":2,"create_time":1639440000000,"name":"手机通讯","id":13},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":53677709,"name":"mysql_binlog_source","row":0,"ts_ms":1747029841000,"snapshot":"false","db":"realtime_v2","table":"base_category2"},"ts_ms":1747029840060}

        KafkaSource<String> kafkaSource = FlinkSource.getKafkaSource(Constant.topic_log);
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
//        kafkaDs.print();
//        4> {"op":"c","after":{"category1_id":17,"create_time":1639440000000,"name":"健身训练","id":112},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60260558,"name":"mysql_binlog_source","row":0,"ts_ms":1747030130000,"snapshot":"false","db":"realtime_v2","table":"base_category2"},"ts_ms":1747030128936}


        //过滤搜索词 和设备信息
        SingleOutputStreamOperator<JSONObject> word = kafkaDs.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> {
            // 安全获取 page 对象
            JSONObject pageObj = jsonnObj.getJSONObject("page");
            JSONObject common = jsonnObj.getJSONObject("common");

// 先判断 pageObj 和 common 是否为 null，避免后续访问其方法时报 NPE
            if (pageObj == null || common == null) {
                return false;
            }

// 获取字段值
            String itemType = pageObj.getString("item_type");
            String lastPageId = pageObj.getString("last_page_id");
            String md = common.getString("md");
            String item = pageObj.getString("item");

// 判断字段是否为空
            if (itemType == null || lastPageId == null || md == null || item == null) {
                return false;
            }

// 执行实际过滤逻辑
            return "keyword".equals(itemType) && "search".equals(lastPageId);

        });
//word.print()
//    2> {"common":{"ar":"7","os":"Android 10.1","ch":"vivo","is_new":"1","md":"realme Neo2","mid":"mid_373","vc":"v2.0.1","ba":"realme","sid":"e54a7d19-3614-4535-a308-c8ffe2e7b923"},"page":{"page_id":"good_list","item":"心相印纸抽","during_time":11189,"item_type":"keyword","last_page_id":"search"},"displays":[{"pos_seq":0,"item":"3","item_type":"sku_id","pos_id":10},{"pos_seq":1,"item":"34","item_type":"sku_id","pos_id":10},{"pos_seq":2,"item":"33","item_type":"sku_id","pos_id":10},{"pos_seq":3,"item":"3","item_type":"sku_id","pos_id":10},{"pos_seq":4,"item":"10","item_type":"sku_id","pos_id":10},{"pos_seq":5,"item":"18","item_type":"sku_id","pos_id":10},{"pos_seq":6,"item":"6","item_type":"sku_id","pos_id":10},{"pos_seq":7,"item":"3","item_type":"sku_id","pos_id":10},{"pos_seq":8,"item":"29","item_type":"sku_id","pos_id":10},{"pos_seq":9,"item":"29","item_type":"sku_id","pos_id":10},{"pos_seq":10,"item":"30","item_type":"sku_id","pos_id":10},{"pos_seq":11,"item":"10","item_type":"sku_id","pos_id":10},{"pos_seq":12,"item":"3","item_type":"sku_id","pos_id":10},{"pos_seq":13,"item":"28","item_type":"sku_id","pos_id":10},{"pos_seq":14,"item":"26","item_type":"sku_id","pos_id":10},{"pos_seq":15,"item":"9","item_type":"sku_id","pos_id":10},{"pos_seq":16,"item":"10","item_type":"sku_id","pos_id":10},{"pos_seq":17,"item":"9","item_type":"sku_id","pos_id":10},{"pos_seq":18,"item":"35","item_type":"sku_id","pos_id":10},{"pos_seq":19,"item":"26","item_type":"sku_id","pos_id":10},{"pos_seq":20,"item":"31","item_type":"sku_id","pos_id":10}],"ts":1747012501067}
//        ;
//        result_user.connect()


//        订单->明细->sku->品牌->三级品类->二级品类->一级品类->用户-> 日志
        SingleOutputStreamOperator<JSONObject> id = order_info.filter(o -> o.getString("id") != null);
        KeyedStream<JSONObject, String> order_id_key = id.keyBy(o -> o.getString("id"));
        SingleOutputStreamOperator<JSONObject> order_info1 = order_detail.filter(o -> o.getString("order_info") != null);
        KeyedStream<JSONObject, String> order_detail_id_key = order_info1.keyBy(o -> o.getString("order_id"));
        KeyedStream.IntervalJoined<JSONObject, JSONObject, String> between = order_id_key.intervalJoin(order_detail_id_key)
                .between(Time.minutes(-60), Time.minutes(60));
        SingleOutputStreamOperator<JSONObject> process = between.process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject a, JSONObject aa,
                                       ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.
                                               Context context, Collector<JSONObject> collector)
                    throws Exception {
                String order_id = a.getString("id");
                String create_time = a.getString("create_time");
                String user_info = a.getString("user_info");
                String sku_id = aa.getString("sku_id");


                JSONObject result = new JSONObject();
                result.put("order_id", order_id);
                result.put("create_time", create_time);
                result.put("user_info", user_info);
                result.put("sku_id", sku_id);
                // 发送到下游
                collector.collect(result);
            }
        });
        process.print();

    }
}
