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
 * @Package Realtime_v2_Dim.Dim_App
 * @Author zeyang_jia
 * @Date 2025/5/12 10:20
 * @description: 配置流维度数据写入hbase
 */
public class Dim_App extends BaseApp {
    public static void main(String[] args) {
        try {
            new Dim_App().start(9098, 1, "dim_ck_group", Constant.topic_db);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
//            kafkaStrDS.print();
//        {"before":{"id":2451,"consignee":"呼延凤","consignee_tel":"13628539274","total_amount":"37302.10","order_status":"1001","user_id":257,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"157949324577931","trade_body":"TCL 85Q6 85英寸 巨幕私人影院电视 4K超高清 AI智慧屏 全景全面屏 MEMC运动防抖 2+16GB 液晶平板电视机等9件商品","create_time":1747092557000,"operate_time":null,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":18,"activity_reduce_amount":"1199.90","coupon_reduce_amount":"0.00","original_total_amount":"38502.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747697357000},"after":{"id":2451,"consignee":"呼延凤","consignee_tel":"13628539274","total_amount":"37302.10","order_status":"1002","user_id":257,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"157949324577931","trade_body":"TCL 85Q6 85英寸 巨幕私人影院电视 4K超高清 AI智慧屏 全景全面屏 MEMC运动防抖 2+16GB 液晶平板电视机等9件商品","create_time":1747092557000,"operate_time":1747092586000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":18,"activity_reduce_amount":"1199.90","coupon_reduce_amount":"0.00","original_total_amount":"38502.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747697357000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747015814000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_info","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":36283353,"row":0,"thread":444,"query":null},"op":"u","ts_ms":1747015813039,"transaction":null}

        SingleOutputStreamOperator<JSONObject> kafkaDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                String db = jsonObj.getJSONObject("source").getString("db");
                String type = jsonObj.getString("op");
                String data = jsonObj.getString("after");
                if ("realtime_v2".equals(db)
                        && ("c".equals(type)
                        || "u".equals(type)
                        || "d".equals(type)
                        || "r".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    collector.collect(jsonObj);
                }
            }
        });
//        kafkaDs.print();
//        10> {"before":{"id":2641,"consignee":"西门俊峰","consignee_tel":"13232789783","total_amount":"11.00","order_status":"1001","user_id":751,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"563883584774982","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等1件商品","create_time":1747092144000,"operate_time":null,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":14,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"11.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747696944000},"after":{"id":2641,"consignee":"西门俊峰","consignee_tel":"13232789783","total_amount":"11.00","order_status":"1002","user_id":751,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"563883584774982","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等1件商品","create_time":1747092144000,"operate_time":1747092180000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":14,"activity_reduce_amount":"0.00","coupon_reduce_amount":"0.00","original_total_amount":"11.00","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1747696944000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747017323000,"snapshot":"false","db":"realtime_v2","sequence":null,"table":"order_info","server_id":1,"gtid":null,"file":"mysql-bin.000046","pos":39091740,"row":0,"thread":536,"query":null},"op":"u","ts_ms":1747017322457,"transaction":null}
        MySqlSource<String> getmysqlsource = FlinkSource.getmysqlsource("realtime_v2", "table_process_dim");
        DataStreamSource<String> mySQL_source = env.fromSource(getmysqlsource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        mySQL_source.print();
        SingleOutputStreamOperator<CommonTable_Dim> tpds = mySQL_source.map(new MapFunction<String, CommonTable_Dim>() {
            @Override
            public CommonTable_Dim map(String s) throws Exception {
                // 将输入的 JSON 字符串解析为 JSONObject
                JSONObject jsonObject = JSON.parseObject(s);
                // 获取操作类型 "op"
                String op = jsonObject.getString("op");
                //局部变量必须初始化
                CommonTable_Dim commonTable = null;

                // 根据操作类型决定是从 "before" 还是 "after" 字段中提取数据
                if ("d".equals(op)) {
                    // 如果是 "d"（删除），从 "before" 中获取 CommonTable 对象
                    commonTable = jsonObject.getObject("before", CommonTable_Dim.class);
                } else {
                    // 其他操作类型（如 "c"、"u"、"r"）从 "after" 中获取 CommonTable 对象
                    commonTable = jsonObject.getObject("after", CommonTable_Dim.class);
                }

                // 设置操作类型并返回对象
                commonTable.setOp(op);
                return commonTable;
            }

        });
//        tpds.print();
//        CommonTable_Dim(sourceTable=sku_info, sinkTable=dim_sku_info, sinkColumns=id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time, sinkFamily=info, sinkRowKey=id, op=r)
        tpds.map(
                new RichMapFunction<CommonTable_Dim, CommonTable_Dim>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn = Hbaseutlis.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutlis.closeHBaseConnection(hbaseconn);
                    }



                    @Override
                    public CommonTable_Dim map(CommonTable_Dim commonTable) throws Exception {
                        String op = commonTable.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            Hbaseutlis.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            Hbaseutlis.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            Hbaseutlis.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                            Hbaseutlis.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });
        MapStateDescriptor<String, CommonTable_Dim> tableMapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, CommonTable_Dim.class);
        BroadcastStream<CommonTable_Dim> broadcast = tpds.broadcast(tableMapStateDescriptor);

        BroadcastConnectedStream<JSONObject, CommonTable_Dim> connects = kafkaDs.connect(broadcast);
        //处理流合并
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable_Dim>> dimDS = connects.
                process(
                new Tablepeocessfcation(tableMapStateDescriptor)
        );
//        2> ({"op":"u","dic_code":"1103","dic_name":"iiii"},CommonTable(sourceTable=base_dic, sinkTable=dim_base_dic, sinkColumns=dic_code,dic_name, sinkFamily=info, sinkRowKey=dic_code, op=c))
//        realtime_v2:dim_financial_sku_cost
        dimDS.print();
        dimDS.addSink(new FlinkSinkHbase());

    }
}
