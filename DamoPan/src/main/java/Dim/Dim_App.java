package Dim;

import Base.BaseApp;
import Bean.CommonTable_Dim;
import Constant.Constant;
import Utils.FlinkSinkHbase;
import Utils.FlinkSource;
import Utils.Hbaseutlis;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * 配置流维度数据写入HBase应用
 * 主要功能：从Kafka获取业务数据，结合配置表信息，将维度数据写入HBase
 */
public class Dim_App extends BaseApp {
    public static void main(String[] args) {
        try {
            // 启动应用，设置并行度、消费者组和Kafka主题
            new Dim_App().start(9098, 1, "dim_ck_group", Constant.topic_db);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 处理Kafka流数据，过滤出需要的业务数据
        SingleOutputStreamOperator<JSONObject> kafkaDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                // 解析JSON消息
                JSONObject jsonObj = JSON.parseObject(s);
                // 获取数据库名
                String db = jsonObj.getJSONObject("source").getString("db");
                // 获取操作类型
                String type = jsonObj.getString("op");
                // 获取变更后的数据
                String data = jsonObj.getString("after");

                // 过滤条件：
                // 1. 数据库为realtime_v2
                // 2. 操作类型为增删改查
                // 3. 变更数据不为空
                if ("realtime_v2".equals(db)
                        && ("c".equals(type)
                        || "u".equals(type)
                        || "d".equals(type)
                        || "r".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    // 满足条件的数据继续向下游传递
                    collector.collect(jsonObj);
                }
            }
        });

        // 从MySQL获取配置表数据（表结构信息）
        MySqlSource<String> getmysqlsource = FlinkSource.getmysqlsource("realtime_v2", "table_process_dim");
        DataStreamSource<String> mySQL_source = env.fromSource(getmysqlsource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 解析配置表数据为CommonTable_Dim对象
        SingleOutputStreamOperator<CommonTable_Dim> tpds = mySQL_source.map(new MapFunction<String, CommonTable_Dim>() {
            @Override
            public CommonTable_Dim map(String s) throws Exception {
                // 解析JSON字符串
                JSONObject jsonObject = JSON.parseObject(s);
                // 获取操作类型
                String op = jsonObject.getString("op");
                CommonTable_Dim commonTable = null;

                // 根据操作类型决定从before还是after字段获取数据
                if ("d".equals(op)) {
                    // 删除操作从before获取数据
                    commonTable = jsonObject.getObject("before", CommonTable_Dim.class);
                } else {
                    // 其他操作从after获取数据
                    commonTable = jsonObject.getObject("after", CommonTable_Dim.class);
                }

                // 设置操作类型并返回对象
                commonTable.setOp(op);
                return commonTable;
            }
        });

        // 根据配置表信息动态管理HBase表结构
        tpds.map(
                new RichMapFunction<CommonTable_Dim, CommonTable_Dim>() {
                    // HBase连接对象
                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化HBase连接
                        hbaseconn = Hbaseutlis.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭HBase连接
                        Hbaseutlis.closeHBaseConnection(hbaseconn);
                    }

                    @Override
                    public CommonTable_Dim map(CommonTable_Dim commonTable) throws Exception {
                        // 获取操作类型
                        String op = commonTable.getOp();
                        // 获取HBase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        // 获取HBase表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");

                        // 根据配置表的操作类型执行相应的HBase表操作
                        if ("d".equals(op)) {
                            // 删除配置时，删除对应的HBase表
                            Hbaseutlis.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            // 读取或新增配置时，创建对应的HBase表
                            Hbaseutlis.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            // 修改配置时，先删除再创建HBase表
                            Hbaseutlis.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                            Hbaseutlis.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });

        // 定义广播状态描述符，用于存储配置信息
        MapStateDescriptor<String, CommonTable_Dim> tableMapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, CommonTable_Dim.class);

        // 将配置流广播
        BroadcastStream<CommonTable_Dim> broadcast = tpds.broadcast(tableMapStateDescriptor);

        // 将业务流与广播配置流连接
        BroadcastConnectedStream<JSONObject, CommonTable_Dim> connects = kafkaDs.connect(broadcast);

        // 处理连接流，关联业务数据与配置信息
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable_Dim>> dimDS = connects
                .process(new Tablepeocessfcation(tableMapStateDescriptor));

        // 打印结果并写入HBase
        dimDS.print();
        dimDS.addSink(new FlinkSinkHbase());
    }
}