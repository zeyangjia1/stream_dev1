package Base;
import Utils.FlinkSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

/**
 * @Package Base.BaseApp
 * @Author zeyang_jia
 * @Date 2025/5/12 9:33
 * @description:
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception
    {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(5000L,
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup
                (CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
//                Time.days(30),Time.seconds(3)));


        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///D:/idea_work/Stream_dev/realtime_v2/src/main/java/CK" + ckAndGroupId);

        KafkaSource<String> kafkaSource = FlinkSource.getKafkaSource(topic);
        DataStreamSource<String> kafkaSource2 = env.fromSource(kafkaSource,
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
        handle(env,kafkaSource2);
        env.execute();
    }
    public abstract  void handle
            (StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}

