package DWD.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package DWD.func.AggregateUserDataProcessFunction
 * @Author zeyang_jia
 * @Date 2025/5/14 18:31
 * @description: 按用户ID（Key）聚合设备信息和搜索行为数据的KeyedProcessFunction
 * 功能：统计每个用户的PV（页面浏览量），并收集设备属性（OS、渠道等）和搜索词的去重集合
 */
public class AggregateUserDataProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    // 声明状态变量（ transient 表示不参与序列化，需在open方法中初始化）
    private transient ValueState<Long> pvState;          // PV计数器状态（单值状态）
    private transient MapState<String, Set<String>> fieldsState; // 字段集合状态（键值对状态，存储各字段的去重值集合）


    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV计数器状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class) // 状态名称：pv-state，类型：Long
        );

        // 初始化字段集合状态（需显式指定泛型类型信息，避免类型擦除）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",            // 状态名称：fields-state
                        Types.STRING,              // 键类型：String（字段名）
                        TypeInformation.of(new TypeHint<Set<String>>() {}) // 值类型：Set<String>（字段值集合）
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor); // 获取MapState实例
    }


    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV计数
        // 读取当前PV值，若为null则初始化为1，否则加1
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv); // 将更新后的PV值写入状态


        // 提取字段值
        JSONObject deviceInfo = value.getJSONObject("deviceInfo"); // 获取设备信息JSON
        String os = deviceInfo.getString("os");                     // 操作系统
        String ch = deviceInfo.getString("ch");                     // 渠道（Channel）
        String md = deviceInfo.getString("md");                     // 设备型号（Model）
        String ba = deviceInfo.getString("ba");                     // 品牌（Brand）
        // 可选字段：若存在搜索词则提取，否则为null
        String searchItem = value.containsKey("search_item") ? value.getString("search_item") : null;


        // 更新字段集合状态
        updateField("os", os);     // 更新操作系统集合
        updateField("ch", ch);     // 更新渠道集合
        updateField("md", md);     // 更新设备型号集合
        updateField("ba", ba);     // 更新品牌集合
        if (searchItem != null) {  // 仅当搜索词存在时更新
            updateField("search_item", searchItem);
        }


        //  构建输出结果
        JSONObject output = new JSONObject();
        output.put("uid", value.getString("uid"));           // 保留用户ID
        output.put("ts", value.getLong("ts"));               // 保留时间戳
        output.put("pv", pv);                                // 输出PV计数
        // 将各字段的去重集合转换为逗号分隔的字符串
        output.put("os", String.join(",", getField("os")));
        output.put("ch", String.join(",", getField("ch")));
        output.put("md", String.join(",", getField("md")));
        output.put("ba", String.join(",", getField("ba")));
        output.put("search_item", String.join(",", getField("search_item")));

        out.collect(output); // 输出聚合结果
    }

    // 辅助方法：更新字段值集合
    /**
     * 向指定字段的集合中添加值（自动去重）
     * @param field 字段名（如"os"、"ch"）
     * @param value 字段值（如"Android"、"AppStore"）
     */
    private void updateField(String field, String value) throws Exception {
        // 读取当前集合，若不存在则创建空集合
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        if (value != null) { // 忽略null值（避免无效数据存入状态）
            set.add(value); // 自动去重（HashSet特性）
        }
        fieldsState.put(field, set); // 将更新后的集合写入状态
    }

    // 辅助方法：获取字段值集合
    /**
     * 获取指定字段的集合（安全处理null情况）
     * @param field 字段名
     * @return 字段值集合（不可变视图，避免外部修改状态）
     */
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }
}