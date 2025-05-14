package DWD.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package DWD.func.PriceTime
 * @Author zeyang_jia
 * @Date 2025/5/14 22:57
 * @description: 时间 和   金额打分
 */
public class PriceTime extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String priceRange = jsonObject.getString("priceRange");
        String time_period = jsonObject.getString("time_period");

        if (priceRange.equals("高价商品")) {
            jsonObject.put("price_18_24", 0.1 * 0.15);
            jsonObject.put("price_25_29", 0.2 * 0.15);
            jsonObject.put("price_30_34", 0.3 * 0.15);
            jsonObject.put("price_35_39", 0.4 * 0.15);
            jsonObject.put("price_40_49", 0.5 * 0.15);
            jsonObject.put("price_50", 0.6 * 0.15);

        } else if (priceRange.equals("中价商品")) {
            jsonObject.put("price_18_24", 0.2 * 0.15);
            jsonObject.put("price_25_29", 0.3 * 0.15);
            jsonObject.put("price_30_34", 0.4 * 0.15);
            jsonObject.put("price_35_39", 0.5 * 0.15);
            jsonObject.put("price_40_49", 0.6 * 0.15);
            jsonObject.put("price_50", 0.7 * 0.15);
        } else if (priceRange.equals("低价商品")){
            jsonObject.put("price_18_24", 0.8 * 0.15);
            jsonObject.put("price_25_29", 0.6 * 0.15);
            jsonObject.put("price_30_34", 0.4 * 0.15);
            jsonObject.put("price_35_39", 0.3 * 0.15);
            jsonObject.put("price_40_49", 0.2 * 0.15);
            jsonObject.put("price_50", 0.1 * 0.15);
        }else if (time_period.equals("凌晨")){
            jsonObject.put("time_18_24", 0.2 * 0.1);
            jsonObject.put("time_25_29", 0.1 * 0.1);
            jsonObject.put("time_30_34", 0.1 * 0.1);
            jsonObject.put("time_35_39", 0.1 * 0.1);
            jsonObject.put("time_40_49", 0.1 * 0.1);
            jsonObject.put("time_50", 0.1 * 0.15);
        }else if (time_period.equals("早晨")){
            jsonObject.put("time_18_24", 0.1 * 0.1);
            jsonObject.put("time_25_29", 0.1 * 0.1);
            jsonObject.put("time_30_34", 0.1 * 0.1);
            jsonObject.put("time_35_39", 0.1 * 0.1);
            jsonObject.put("time_40_49", 0.2 * 0.1);
            jsonObject.put("time_50", 0.2 * 0.1);
        }else if (time_period.equals("上午")){
            jsonObject.put("time_18_24", 0.2 * 0.1);
            jsonObject.put("time_25_29", 0.2 * 0.1);
            jsonObject.put("time_30_34", 0.2 * 0.1);
            jsonObject.put("time_35_39", 0.2 * 0.1);
            jsonObject.put("time_40_49", 0.3 * 0.1);
            jsonObject.put("time_50", 0.4 * 0.1);
        }else if (time_period.equals("中午")){
            jsonObject.put("time_18_24", 0.4 * 0.1);
            jsonObject.put("time_25_29", 0.4 * 0.1);
            jsonObject.put("time_30_34", 0.4 * 0.1);
            jsonObject.put("time_35_39", 0.4 * 0.1);
            jsonObject.put("time_40_49", 0.4 * 0.1);
            jsonObject.put("time_50", 0.3 * 0.1);

        }else if (time_period.equals("下午")){
            jsonObject.put("time_18_24", 0.4 * 0.1);
            jsonObject.put("time_25_29", 0.5 * 0.1);
            jsonObject.put("time_30_34", 0.6 * 0.1);
            jsonObject.put("time_35_39", 0.6 * 0.1);
            jsonObject.put("time_40_49", 0.6 * 0.1);
            jsonObject.put("time_50", 0.4 * 0.1);

        }else if (time_period.equals("晚上")){
            jsonObject.put("time_18_24", 0.8 * 0.1);
            jsonObject.put("time_25_29", 0.7 * 0.1);
            jsonObject.put("time_30_34", 0.6 * 0.1);
            jsonObject.put("time_35_39", 0.5 * 0.1);
            jsonObject.put("time_40_49", 0.4 * 0.1);
            jsonObject.put("time_50", 0.3 * 0.1);

        }else if (time_period.equals("夜间")){
            jsonObject.put("time_18_24", 0.9 * 0.1);
            jsonObject.put("time_25_29", 0.7 * 0.1);
            jsonObject.put("time_30_34", 0.5 * 0.1);
            jsonObject.put("time_35_39", 0.3 * 0.1);
            jsonObject.put("time_40_49", 0.2 * 0.1);
            jsonObject.put("time_50", 0.1 * 0.1);

        }

        return jsonObject;
    }
}
