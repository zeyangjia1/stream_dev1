package DWD.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

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


        JSONObject result = new JSONObject();
        String age_group = jsonObject.getString("age_group");
        if (priceRange != null && time_period != null
        &&jsonObject.containsKey("priceRange") && jsonObject.containsKey("time_period")
        ){
            if (age_group != null && age_group.equals("18-24")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_18_24", round(0.2 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_18_24", round(0.1 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_18_24", round(0.2 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_18_24", round(0.4 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_18_24", round(0.4 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_18_24", round(0.8 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_18_24", round(0.9 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_18_24", round(0.1 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_18_24", round(0.2 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_18_24", round(0.8 * 0.15));
                }
            }



            if (age_group != null && age_group.equals("25-29")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_25-29", round(0.1 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_25-29", round(0.1 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_25-29", round(0.2 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_25-29", round(0.4 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_25-29", round(0.5 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_25-29", round(0.7 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_25-29", round(0.7 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_25-29", round(0.2 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_25-29", round(0.4 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_25-29", round(0.6 * 0.15));
                }
            }



            if (age_group != null && age_group.equals("30-34")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_30.14", round(0.1 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_30.14", round(0.1 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_30.14", round(0.2 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_30.14", round(0.4 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_30.14", round(0.5 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_30.14", round(0.6 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_30.14", round(0.5 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_30.14", round(0.1 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_30.14", round(0.6 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_30.14", round(0.4 * 0.15));
                }
            }



            if (age_group != null && age_group.equals("35-39")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_35-39", round(0.1 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_35-39", round(0.1 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_35-39", round(0.2 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_35-39", round(0.4 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_35-39", round(0.5 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_35-39", round(0.5 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_35-39", round(0.1 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_35-39", round(0.4 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_35-39", round(0.7 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_35-39", round(0.1 * 0.15));
                }
            }



            if (age_group != null && age_group.equals("40-49")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_40-49", round(0.1 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_40-49", round(0.2 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_40-49", round(0.1 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_40-49", round(0.4 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_40-49", round(0.5 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_40-49", round(0.4 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_40-49", round(0.2 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_40-49", round(0.5 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_40-49", round(0.8 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_40-49", round(0.2 * 0.15));
                }
            }



            if (age_group != null && age_group.equals("50")) {
                // 时间
                if (time_period.equals("凌晨")) {
                    result.put("te_50", round(0.1 * 0.1));
                } else if (time_period.equals("早晨")) {
                    result.put("te_50", round(0.1 * 0.1));
                } else if (time_period.equals("上午")) {
                    result.put("te_50", round(0.4 * 0.1));
                } else if (time_period.equals("中午")) {
                    result.put("te_50", round(0.1 * 0.1));
                }else if (time_period.equals("下午")) {
                    result.put("te_50", round(0.4 * 0.1));
                }else if (time_period.equals("晚上")) {
                    result.put("te_50", round(0.1 * 0.1));
                }else if (time_period.equals("夜间")) {
                    result.put("te_50", round(0.1 * 0.1));
                }
                //价格
                if (priceRange.equals("高价商品")) {
                    result.put("tl_50", round(0.6 * 0.15));
                } else if (priceRange.equals("中价商品")) {
                    result.put("tl_50", round(0.7 * 0.15));
                } else if (priceRange.equals("低价商品")) {
                    result.put("tl_50", round(0.1 * 0.15));
                }
            }
        }


        jsonObject.put("result1",result);
        return jsonObject;
    }
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
