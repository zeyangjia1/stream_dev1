package Base;

import Utils.Sqlutil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package Base.BasesqlApp
 * @Author ayang
 * @Date 2025/4/11 11:25
 * @description: SQL处理基类，提供Flink流处理环境初始化和基础表创建功能
 */
public abstract class BasesqlApp {

    /**
     * 启动Flink流处理应用
     * @param port Web UI端口
     * @param parallelism 并行度设置
     * @param ck 检查点路径（当前未使用）
     */
    public void start(int port, int parallelism, String ck) {
        // TODO 1. 基本环境准备
        // 1.1 指定流处理环境，配置Web UI端口
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 设置作业并行度
        env.setParallelism(parallelism);

        // 1.3 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点相关的设置
        // 启用检查点，设置间隔时间和一致性模式
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置失败率重启策略：30天内最多3次失败，每次重启间隔3秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        // 2.2 设置检查点超时时间为6秒
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 2.3 任务取消时保留检查点文件
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.4 设置两个检查点之间最小间隔2秒，避免资源竞争
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        // 2.5 重复设置重启策略（可能是代码冗余）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        // 2.6 使用内存状态后端（适用于开发测试，生产环境建议使用 RocksDB）
        env.setStateBackend(new HashMapStateBackend());

        // 2.7 设置Hadoop用户（已注释，实际未启用）
        // System.setProperty("HADOOP_USER_NAME","root");

        // TODO 3. 业务处理逻辑（由子类实现）
        handle(tableEnv);
    }

    /**
     * 业务逻辑处理抽象方法
     * @param tableEnv 表执行环境
     */
    public abstract void handle(StreamTableEnvironment tableEnv);

    /**
     * 从Kafka topic_db主题读取数据，创建动态表
     * @param tableEnv 表执行环境
     * @param groupId Kafka消费者组ID
     */
    public void readOdsDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("" +
                "CREATE TABLE topic_table_v1 (\n" +
                "  before MAP<string,string>,\n" +  // 变更前的数据
                "  after Map<String,String>,\n" +   // 变更后的数据
                "  source  Map<String,String>,\n" + // 数据源信息
                "  op  String,\n" +                // 操作类型（insert/update/delete）
                "  ts_ms  bigint,\n" +             // 事件时间戳（毫秒）
                "  proc_time  AS proctime(),\n " + // 处理时间属性
                "  et AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" + // 事件时间转换
                "  WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" + // 水位线设置（延迟3秒）
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db_v2',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" + // 从最早的偏移量开始消费
                "  'format' = 'json'\n" +
                ")");
        // tableEnv.executeSql("select * from topic_db").print(); // 调试用，已注释
    }

    /**
     * 从HBase的字典表中读取数据，创建动态表
     * @param tableEnv 表执行环境
     */
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +           // 字典编码
                " info ROW<dic_name string>,\n" + // 字典信息（包含名称）
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" + // 指定主键
                ") " + Sqlutil.getHBaseDDL("dim_base_dic")); // 通过工具类生成HBase连接DDL
        // tableEnv.executeSql("select * from base_dic").print(); // 调试用，已注释
    }
}