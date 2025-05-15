package DWD;

import Utils.FlinkSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * @Package DWD.AgeThemeScoringAggregation
 * @Author zeyang_jia
 * @Date 2025/5/15 16:48
 * @description: 年龄主题下标签打分后聚合
 */
public class AgeThemeScoringAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
//                Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());


        FlinkSource  flinkSource = new FlinkSource();


        KafkaSource<String> totalSource_string = flinkSource.getKafkaSource("TotalSource_string");
        DataStreamSource<String> TotalTimeSource = env.fromSource(totalSource_string,
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

        KafkaSource<String> SearchSource_string = flinkSource.getKafkaSource("SearchSource_string");
        DataStreamSource<String> SearchSource = env.fromSource(SearchSource_string,
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
//        4> {"birthday":"1988-12-08","device_35_39":0.04,"search_25_29":0,"pv":23,"search_40_49":0,"uid":"45","unit_height":"cm","ear":1980,"md":"iPhone 14,realme Neo2,vivo IQOO Z6x ,xiaomi 12 ultra ","search_18_24":0,"phone_num":"13344779847","search_item":"","email":"21aq6t@googlemail.com","height":"161","search_30_34":0,"uname":"韩致树","os":"iOS,Android","device_50":0.02,"ch":"Appstore,xiaomi,web,vivo","age_group":"35-39岁","weight":"73","device_30_34":0.05,"device_18_24":0.07,"search_50":0,"device_25_29":0.06,"login_name":"21aq6t","user_level":"1","judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"unit_weight":"kg","ts_ms":1747295903594,"ba":"iPhone,xiaomi,realme,vivo"}

        KafkaSource<String>TmCmSource_string = flinkSource.getKafkaSource("CmTmSource_string");
        DataStreamSource<String> TmCmSource = env.fromSource(TmCmSource_string,
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
//        TotalTimeSource.print("TotalTimeSource");
//        SearchSource.print("SearchSource");
//        TmCmSource.print("TmCmSource");



//        TmCmSource:3> {"birthday":"1970-10-08","category_name":"手机","uname":"范颖","gender":"F","age_group":"50","weight":"57","tm_name":"苹果","uid":"32","login_name":"orpu36szbp","unit_height":"cm","ear":1970,"user_level":"1","phone_num":"13634137917","unit_weight":"kg","email":"orpu36szbp@126.com","ts_ms":1747295911160,"result2":{"tm_50":0.02,"cm_50":0.21},"height":"147"}
//        TotalTimeSource:2> {"birthday":"1999-02-15","create_time":"1747304443000","uname":"唐以建","gender":"M","age_group":"25-29","weight":"73","uid":"191","login_name":"l463uohvbw","unit_height":"cm","total_amount":"10799.10","ear":1990,"user_level":"1","phone_num":"13495892391","unit_weight":"kg","order_id":"778","priceRange":"高价商品","email":"l463uohvbw@sina.com","ts_ms":1747296061850,"time_period":"晚上","height":"159","result1":{"te_25-29":0.07,"tl_25-29":0.03}}
//        SearchSource:2> {"birthday":"1979-09-08","device_25-29":0.07,"gender":"M","pv":5,"device_40-49":0.04,"search_30-34":0,"sum":0.04,"search_18-24":0,"uid":"29","unit_height":"cm","ear":1970,"md":"xiaomi 13","phone_num":"13525978189","search_item":"","search_25-29":0,"email":"l7il2i4mr@aol.com","device_30-34":0.06,"search_40-49":0,"height":"176","device_18-24":0.08,"uname":"郝力明","os":"Android","device_50":0.03,"ch":"wandoujia","age_group":"40-49","weight":"89","search_35-39":0,"search_50":0,"device_35-39":0.05,"login_name":"l7il2i4mr","user_level":"2","judge_os":"Android","unit_weight":"kg","ts_ms":1747295944955,"ba":"xiaomi"}

        //{"birthday":"1983-08-15","create_time":"1747351762000","uname":"诸葛波宁","age_group":"40-49","weight":"62","uid":"246","unit_height":"cm","total_amount":"11749.00","ear":1980,"unit_weight":"kg","order_id":"751","priceRange":"高价商品","ts_ms":1747296087562,"time_period":"早晨","height":"191","result1":{"te_40-49":0.02,"tl_40-49":0.075,"SearchSum":0.15}}
        SingleOutputStreamOperator<JSONObject> TotalTime = TotalTimeSource.map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> Search = SearchSource.map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> TmCm = TmCmSource.map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> TotalSearch = TotalTime.keyBy(o -> o.getString("uid"))
                .intervalJoin(Search.keyBy(o -> o.getString("uid")))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject object = new JSONObject();
                        jsonObject.remove("login_name");
                        jsonObject.remove("user_level");
                        jsonObject.remove("email");
                        jsonObject.remove("phone_num");
                        object.putAll(jsonObject);
                        Double sum = jsonObject2.getDouble("sum");
                        JSONObject result1 = object.getJSONObject("result1");
                        result1.put("SearchSum", sum);
                        collector.collect(object);
                    }
                });

        //AllTotal> {"birthday":"2002-10-15","create_time":"1747310828000","uname":"费晶","gender":"F","age_group":"18-24","weight":"41","uid":"211","unit_height":"cm","total_amount":"8197.00","ear":2000,"unit_weight":"kg","order_id":"602","priceRange":"高价商品","ts_ms":1747295622461,"time_period":"晚上","height":"160","result1":{"tl_18_24":0.015,"cm_18_24":0.06,"te_18_24":0.08,"SearchSum":0.07,"tm_18_24":0.18}}
        SingleOutputStreamOperator<JSONObject> AllTotal = TotalSearch.keyBy(o -> o.getString("uid"))
                .intervalJoin(TmCm.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-5), Time.hours(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result2 = jsonObject2.getJSONObject("result2");
                        JSONObject result1 = jsonObject.getJSONObject("result1");
                        result1.putAll(result2);
                        collector.collect(jsonObject);
                    }
                });

        //Age-->> {"birthday":"2002-10-15","create_time":"1747310828000","uname":"费晶","gender":"F","age_group":"18-24","weight":"41","uid":"211","unit_height":"cm","total_amount":"8197.00","ear":2000,"unit_weight":"kg","new_ageGroup":"40-49","order_id":"602","priceRange":"高价商品","ts_ms":1747295622461,"time_period":"晚上","height":"160","result1":{"tl_18_24":0.015,"allSum":0.385,"cm_30_34":0.12,"te_18_24":0.08,"SearchSum":0.07,"tm_30_34":0.1}}
        SingleOutputStreamOperator<JSONObject> Age = AllTotal.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                BigDecimal sum = BigDecimal.ZERO;
                JSONObject result1 = jsonObject.getJSONObject("result1");
                for (String s : result1.keySet()) {
                    BigDecimal bigDecimal = result1.getBigDecimal(s);
                    sum = sum.add(bigDecimal != null ? bigDecimal : BigDecimal.ZERO);
                }
                result1.put("allSum", sum);
                sum = sum.setScale(3, RoundingMode.HALF_UP);
                double aDouble = sum.doubleValue();
                if (aDouble >= 0.75) {
                    jsonObject.put("new_ageGroup", "18-24");
                } else if (aDouble >= 0.69) {
                    jsonObject.put("new_ageGroup", "25-29");
                } else if (aDouble >= 0.585) {
                    jsonObject.put("new_ageGroup", "30-34");
                } else if (aDouble >= 0.47) {
                    jsonObject.put("new_ageGroup", "35-39");
                } else if (aDouble >= 0.365) {
                    jsonObject.put("new_ageGroup", "40-49");
                } else if (aDouble >= 0.26) {
                    jsonObject.put("new_ageGroup", "50+");
                } else {
                    jsonObject.put("new_ageGroup", "0-17");
                }
                return jsonObject;
            }
        });
        Age.print("Age-->");


        env.execute();

    }
}
