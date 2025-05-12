package Base;

import Utils.FlinkSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.days(30),Time.seconds(3)));


        env.setStateBackend(new HashMapStateBackend());


        KafkaSource<String> kafkaSource = FlinkSource.getKafkaSource(topic);
        DataStreamSource<String> kafkaDs =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                        "kafkaSource");

        handle(env,kafkaDs);
        env.execute();
    }
    public abstract  void handle
            (StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}

