package DWD;
import Base.BaseApp;
import Constant.Constant;
import func.FilterBloomDeduplicatorFunc_v2;
import Utils.FinkSink;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package DWD.DWD_user_info.java
 * @Author zeyang_jia
 * @Date 2025/5/12 15:09
 * @description: 对用户进行过滤
 */
public class dwd_user_info extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dwd_user_info().start(10051,1,"dwd_user_info:1", Constant.topic_db);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

//        kafkaStrDS.print();
        //在kafka数据中过滤出user_info数据
        SingleOutputStreamOperator<JSONObject> UserInfoDS = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info".equals(jsonObj.getJSONObject("source").getString("table")));
        //在kafka数据中过滤出userInfoSupMsgDS数据
        SingleOutputStreamOperator<JSONObject> userInfoSupMsgDS = kafkaStrDS.map(JSON::parseObject)
                .filter(jsonObj -> "user_info_sup_msg".equals(jsonObj.getJSONObject("source").getString("table")));
//        UserInfoDS.print();
        //对字段
        //把birthday变换成yyyy-MM-dd
        SingleOutputStreamOperator<JSONObject> result_user = UserInfoDS.map(new RichMapFunction<JSONObject, JSONObject>() {
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
                        String daysSinceBaseStr


                                = birthdayValue.toString();

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
//        mapDs.print();
        SingleOutputStreamOperator<JSONObject> filterDs = result_user.keyBy(jsonObj -> jsonObj.getJSONObject("after").getLong("id"))
                .filter(new FilterBloomDeduplicatorFunc_v2(1000000, 0.01));
//        filterDs.print();

        SingleOutputStreamOperator<JSONObject> UserInfo = filterDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Long ts_ms = jsonObject.getLong("ts_ms");

                if (jsonObject.containsKey("after")) {
                    result.put("uid", after.getString("id"));
                    result.put("uname", after.getString("name"));
                    result.put("gender", after.getString("gender"));
                    result.put("user_level", after.getString("user_level"));
                    result.put("login_name", after.getString("login_name"));
                    result.put("phone_num", after.getString("phone_num"));
                    result.put("email", after.getString("email"));
                    result.put("birthday", after.getString("birthday"));
                    result.put("ts_ms",ts_ms);
                    String ear = after.getString("birthday");
                    //截取前4个
                    Integer intValue = new Integer(ear.substring(0, 4));
                    result.put("ear",(intValue /10)*10);
                }
                return result;
            }
        });
//        UserInfo.print();
// 对userInfoSupMsgDS进行处理
        SingleOutputStreamOperator<JSONObject> userInfoSumMsgBean = userInfoSupMsgDS.keyBy(data -> data.getJSONObject("after").getLong("uid")).map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                if (jsonObject.containsKey("after")) {
                    result.put("uid", after.getString("uid"));
                    result.put("gender", after.getString("gender"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getString("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getLongValue("height"));
                }
                return result;
            }
        });
//        userInfoSumMsgBean.print();


        // 让 UserInfo 和 userInfoSumMsgBean 进行join
        //TODO 5.关联身高体重
        SingleOutputStreamOperator<JSONObject> name = UserInfo
                .keyBy(o -> o.getInteger("uid"))
                .intervalJoin(userInfoSumMsgBean.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject j1, JSONObject j2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        j1.put("height", j2.getString("height"));
                        j1.put("unit_height", j2.getString("unit_height"));
                        j1.put("weight", j2.getString("weight"));
                        j1.put("unit_weight", j2.getString("unit_weight"));
                        collector.collect(j1);
                    }
                })
                .uid("intervalJoin")
                .name("intervalJoin");
//        name.print();
//        {"birthday":"1981-01-12",
//        "uname":"俞旭",
//        "gender":"M",
//        "weight":"64",
//        "uid":"565",
//        "login_name":"n1tt8wi",
//        "unit_height":"cm",
//        "ear":1980,
//        "user_level":"1",
//        "phone_num":"13115549881",
//        "unit_weight":"kg",
//        "email":"o1ja2m@ask.com",
//        "ts_ms":0,
//        "height":"168"}


        SingleOutputStreamOperator<String> map = name.map(data -> data.toJSONString());
//        map.print();

        map.sinkTo(FinkSink.getkafkasink(Constant.dmp_user_info));


    }
}
