package Dim;

import Bean.CommonTable_Dim;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import Utils.JdbsUtils;
import java.sql.Connection;
import java.util.*;
/**
 * @Package realtime_Dim.flinkfcation.Tablepeocessfcation
 * @Author a_yang
 * @Date 2025/4/10 10:55
 * @description: aa
 */
public class Tablepeocessfcation extends BroadcastProcessFunction<JSONObject, CommonTable_Dim, Tuple2<JSONObject, CommonTable_Dim>>{

    private Map<String, CommonTable_Dim> configMap = new HashMap<>();
    private  MapStateDescriptor<String, CommonTable_Dim> tableMapStateDescriptor;

    public Tablepeocessfcation(MapStateDescriptor<String, CommonTable_Dim> tableMapStateDescriptor) {
        this.tableMapStateDescriptor = tableMapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbsUtils.getMySQLConnection();
        List<CommonTable_Dim> commonTables = JdbsUtils.queryList(mySQLConnection, "select * from realtime_v2.table_process_dim", CommonTable_Dim.class);
        for (CommonTable_Dim commonTable : commonTables) {
            configMap.put(commonTable.getSourceTable(),commonTable);
        }
        JdbsUtils.closeMySQLConnection(mySQLConnection);

    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, CommonTable_Dim, Tuple2<JSONObject, CommonTable_Dim>>.
            ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, CommonTable_Dim>> collector) throws Exception {

        String table = jsonObject.getJSONObject("source").getString("table");
        ReadOnlyBroadcastState<String, CommonTable_Dim> broadcastState = readOnlyContext.getBroadcastState(tableMapStateDescriptor);
        CommonTable_Dim tableProcessDim = broadcastState.get(table);
        if (tableProcessDim != null) {
            JSONObject dataJsonObj = jsonObject.getJSONObject("after");
            String sinkColumns = tableProcessDim.getSinkColumns();

            deletenotneetclomns(dataJsonObj, sinkColumns);
            String type = jsonObject.getString("op");
            dataJsonObj.put("op", type);
            collector.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }
    }




    @Override
public void processBroadcastElement(CommonTable_Dim commonTable_dim, BroadcastProcessFunction<JSONObject, CommonTable_Dim, Tuple2<JSONObject, CommonTable_Dim>>.Context ctx, Collector<Tuple2<JSONObject, CommonTable_Dim>> collector) throws Exception {
    // 假设 tp 是传入的对象或从上下文中获取的，此处补充合理定义
    CommonTable_Dim tp = commonTable_dim;

    String op = tp.getOp();
    BroadcastState<String, CommonTable_Dim> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);
    String sourceTable = tp.getSourceTable();

    if ("d".equals(op)) {
        broadcastState.remove(sourceTable);
        configMap.remove(sourceTable);
    } else {
        broadcastState.put(sourceTable, tp);
        configMap.put(sourceTable, tp);
    }
}


    private void deletenotneetclomns(JSONObject dataJsonObj, String sinkColumns) {
        if (sinkColumns == null || sinkColumns.isEmpty()) {
            return; // 防止 NullPointerException
        }

        List<String> allowedKeys = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(e -> !allowedKeys.contains(e.getKey()));
    }



    }

