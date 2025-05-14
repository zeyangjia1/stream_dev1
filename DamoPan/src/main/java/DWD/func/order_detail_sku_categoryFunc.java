package DWD.func;

import Utils.Hbaseutlis;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package DWD.func.order_detail_sku_categoryFunc
 * @Author zeyang_jia
 * @Date 2025/5/14 15:46
 * @description: 关联一级品类
 */
public class order_detail_sku_categoryFunc extends RichMapFunction<JSONObject, JSONObject> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = Hbaseutlis.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        Hbaseutlis.closeHBaseConnection(hbaseConn);
    }
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String skuId = jsonObject.getString("category1_id");
        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_category1", skuId, JSONObject.class);
        JSONObject a = new JSONObject();
        a.putAll(jsonObject);
        a.put("category1_name", skuInfoJsonObj.getString("name"));

        return a;
    }
}
