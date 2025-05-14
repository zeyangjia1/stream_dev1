package DWD.func;

import Utils.Hbaseutlis;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package DWD.func.OrderDetailSkuHbase
 * @Author zeyang_jia
 * @Date 2025/5/14 15:32
 * @description: 关联 sku 取三级品类 id
 */
public class OrderDetailSkuHbase extends RichMapFunction<JSONObject, JSONObject> {

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
        String skuId = jsonObject.getString("sku_id");
        JSONObject skuInfoJsonObj = Hbaseutlis.getRow(hbaseConn, "realtime_v2", "dim_sku_info", skuId, JSONObject.class);
        JSONObject a = new JSONObject();
        a.put("uid",jsonObject.getString("uid"));
        a.put("tm_id",skuInfoJsonObj.getString("tm_id"));
        a.put("category3_id", skuInfoJsonObj.getString("category3_id"));
        return a;
    }
}