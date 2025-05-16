package DWD;

import Utils.FlinkSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.Duration;

/**
 * @Package DWD.AgeThemeScoringAggregation
 * @Author zeyang_jia
 * @Date 2025/5/15 16:48
 * @description: 年龄主题下标签打分后聚合
 * 功能说明：
 * 1. 从三个Kafka主题读取用户行为数据（总时间、搜索记录、标签分类）
 * 2. 通过Interval Join实现跨流数据关联（基于用户ID和时间窗口）
 * 3. 聚合各标签分数，根据总分重新计算用户所属年龄组
 * 4. 最终结果输出到CSV文件
 */
public class AgeThemeScoringAggregation {
    public static void main(String[] args) throws Exception {
        //  1. 环境配置
        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //  检查点配置
        // 启用检查点，每5秒触发一次，精确一次语义（保证故障恢复时数据一致性）
        env.enableCheckpointing(5000L, org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        // 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 取消作业时保留检查点
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 检查点最小间隔时间
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 使用HashMap状态后端中小型状态，内存存储
        env.setStateBackend(new HashMapStateBackend());

        //  3. 数据源初始化
        // 自定义数据源工具类（用于获取Kafka源）
        FlinkSource flinkSource = new FlinkSource();

        //  4. 读取Kafka数据
        //  4.1 总时间数据（TotalSource_string主题）
        KafkaSource<String> totalSource_string = flinkSource.getKafkaSource("TotalSource_string_v2");
        DataStreamSource<String> TotalTimeSource = env.fromSource(
                totalSource_string,
                // 水印策略：允许3秒乱序，使用事件中的ts_ms字段作为时间戳
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms"); // 从JSON中提取时间戳
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("解析JSON或获取ts_ms失败: " + event);
                                    return 0L; // 异常时默认时间戳为0
                                }
                            }
                            return 0L;
                        }),
                "kafka_source_total"
        );

        // 4.2 搜索数据（
        KafkaSource<String> SearchSource_string = flinkSource.getKafkaSource("SearchSource_string_v2");
        DataStreamSource<String> SearchSource = env.fromSource(
                SearchSource_string,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("解析JSON或获取ts_ms失败: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }),
                "kafka_source_search"
        );

        //  4.3 标签分类数据
        KafkaSource<String> TmCmSource_string = flinkSource.getKafkaSource("CmTmSource_string_v2"); // 注意主题名是否正确（CmTm vs TmCm）
        DataStreamSource<String> TmCmSource = env.fromSource(
                TmCmSource_string,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("解析JSON或获取ts_ms失败: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }),
                "kafka_source_tmcm"
        );

        //5. 数据处理流程
        // 总时间金额数据转换
        SingleOutputStreamOperator<JSONObject> TotalTime = TotalTimeSource.map(JSONObject::parseObject);
        // 搜索设备数据转换
        SingleOutputStreamOperator<JSONObject> Search = SearchSource.map(JSONObject::parseObject);
        // 品牌类目数据转换
        SingleOutputStreamOperator<JSONObject> TmCm = TmCmSource.map(JSONObject::parseObject);
        //5.2 总时间数据与搜索数据关联（Interval Join）
        SingleOutputStreamOperator<JSONObject> TotalSearch = TotalTime.keyBy(o -> o.getString("uid"))
                .intervalJoin(Search.keyBy(o -> o.getString("uid")))
                .between(Time.seconds(-5), Time.seconds(5)) // 时间窗口范围（左闭右闭）
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject totalEvent, JSONObject searchEvent, Context context, Collector<JSONObject> collector) throws Exception {
                        // 创建新对象合并数据
                        JSONObject merged = new JSONObject();
                        merged.putAll(totalEvent); // 复制总时间和金额数据
                        // 移除无需参与后续计算
                        merged.remove("login_name");
                        merged.remove("user_level");
                        merged.remove("email");
                        merged.remove("phone_num");

                        // 合并搜索数据中的sum字段到result1
                        Double searchSum = searchEvent.getDouble("sum");
                        JSONObject result1 = merged.getJSONObject("result1");
                        result1.put("SearchSum", searchSum); // 添加搜索总分

                        collector.collect(merged); // 输出合并后的数据
                    }
                });

        //  5.3 关联结果与标签分类数据再次关联
        SingleOutputStreamOperator<JSONObject> AllTotal = TotalSearch.keyBy(o -> o.getString("uid"))
                .intervalJoin(TmCm.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-5), Time.hours(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject totalSearchEvent, JSONObject tmCmEvent, Context context, Collector<JSONObject> collector) throws Exception {
                        // 合并标签分类结果（result2）到总结果（result1）
                        JSONObject result2 = tmCmEvent.getJSONObject("result2");
                        JSONObject result1 = totalSearchEvent.getJSONObject("result1");
                        result1.putAll(result2); // 合并键值对

                        collector.collect(totalSearchEvent); // 输出最终合并数据
                    }
                });

        //  5.4 计算总分并重新划分年龄组
        SingleOutputStreamOperator<JSONObject> Age = AllTotal.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                BigDecimal totalScore = BigDecimal.ZERO; // 初始化总分
                JSONObject result1 = jsonObject.getJSONObject("result1");

                // ，计算总分
                for (String key : result1.keySet()) {
                    BigDecimal score = result1.getBigDecimal(key);
                    totalScore = totalScore.add(score != null ? score : BigDecimal.ZERO); // 处理可能的null值
                }

                // 三位小数
                totalScore = totalScore.setScale(3, RoundingMode.HALF_UP);
                result1.put("allSum", totalScore.doubleValue()); // 添加总分到结果

                // 根据总分重新划分年龄组
                double score = totalScore.doubleValue();
                if (score >= 0.75) {
                    jsonObject.put("new_ageGroup", "18-24");
                } else if (score >= 0.69) {
                    jsonObject.put("new_ageGroup", "25-29");
                } else if (score >= 0.585) {
                    jsonObject.put("new_ageGroup", "30-34");
                } else if (score >= 0.47) {
                    jsonObject.put("new_ageGroup", "35-39");
                } else if (score >= 0.365) {
                    jsonObject.put("new_ageGroup", "40-49");
                } else if (score >= 0.26) {
                    jsonObject.put("new_ageGroup", "50+");
                } else {
                    jsonObject.put("new_ageGroup", "0-17"); // 最低分数段
                }

                return jsonObject;
            }
        });

        // 6. 结果输出
//         Age.print("Age-->");

//        Age-->:2> {"birthday":"2007-03-15","create_time":"1747295448000","uname":"戴静淑","age_group":"18-24","weight":"76","uid":"76","unit_height":"cm","constellation":"双鱼座","total_amount":"11749.00","era":"00年代","ear":2000,"unit_weight":"kg","new_ageGroup":"40-49","order_id":"572","priceRange":"高价商品","ts_ms":1747295972435,"time_period":"下午","height":"170","result1":{"tl_18_24":0.015,"cm_18_24":0.06,"allSum":0.365,"te_18_24":0.04,"SearchSum":0.07,"tm_18_24":0.18}}

        // 写入CSV文件并行度设为1避免文件分片
//        Age.writeAsText("D:\\idea_work\\Stream_dev\\DamoPan\\src\\main\\Age.csv").setParallelism(1);

        env.execute("Age Theme Scoring Aggregation Job");
    }
}