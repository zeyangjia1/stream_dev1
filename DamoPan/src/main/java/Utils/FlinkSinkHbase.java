package Utils;

import Bean.CommonTable_Dim;
import Constant.Constant;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package realtime_Dim.flink_fcation.flink_sink_Hbase
 * @Author a_yang
 * @Date 2025/4/10 11:40
 * @description: 写进 hbase
 */
public class FlinkSinkHbase extends RichSinkFunction<Tuple2<JSONObject, CommonTable_Dim>> {
    private Connection hbaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = Hbaseutlis.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        Hbaseutlis.closeHBaseConnection(hbaseConn);
    }

    //将流中数据写入HBase中
    @Override
    public void invoke(Tuple2<JSONObject, CommonTable_Dim> value, Context context)   {
        JSONObject jsonObj = value.f0;
        CommonTable_Dim tableProcessDim = value.f1;
        String op = jsonObj.getString("op");
        jsonObj.remove("op");

        //获取操作的HBase的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取row_key
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        //判断对业务数据维度表进行来了什么操作
        if ("d".equals(op)){
            //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
            Hbaseutlis.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else {
            //如果不是delete,可能的存在
            String sinkFamily = tableProcessDim.getSinkFamily();
            Hbaseutlis.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
        }

    }
}
