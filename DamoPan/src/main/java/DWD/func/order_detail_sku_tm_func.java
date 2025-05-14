package DWD.func;

import Utils.Hbaseutlis;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package DWD.func.order_detail_sku_tm_func
 * @Author zeyang_jia
 * @Date 2025/5/14 16:31
 * @description: 关联 品牌 表
 */
public class order_detail_sku_tm_func extends RichMapFunction<JSONObject, JSONObject> {
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
        String skuId = jsonObject.getString("tm_id");
        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_base_trademark", skuId, JSONObject.class);
        JSONObject a = new JSONObject();
        a.put("uid",jsonObject.getString("uid"));
        a.put("tm_id",jsonObject.getString("tm_id"));

        a.put("tm_name", skuInfoJsonObj.getString("tm_name"));
        return a;
    }
}
