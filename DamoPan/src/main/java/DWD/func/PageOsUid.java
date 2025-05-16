package DWD.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 功能：从埋点JSON数据中提取用户ID、操作系统信息和搜索关键词
 * 输入：埋点原始JSON数据
 * 输出：结构化的JSON数据，包含用户ID、设备信息、时间戳和搜索词
 */
public class PageOsUid extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 1. 创建结果JSON对象
        JSONObject result = new JSONObject();

        // 2. 提取公共字段(common)
        if (jsonObject.containsKey("common")) {
            // 2.1 获取公共信息
            JSONObject common = jsonObject.getJSONObject("common");

            // 2.2 提取用户ID(若不存在则置为-1)
            result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");

            // 2.3 提取时间戳
            result.put("ts", jsonObject.getLongValue("ts"));

            // 2.4 构建设备信息(过滤非设备字段)
            JSONObject deviceInfo = new JSONObject();
            common.remove("sid");   // 移除会话ID
            common.remove("mid");   // 移除设备ID
            common.remove("is_new");// 移除是否新用户标记
            deviceInfo.putAll(common); // 剩余字段作为设备信息
            result.put("deviceInfo", deviceInfo);

            // 3. 提取搜索关键词(如果存在)
            if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                JSONObject pageInfo = jsonObject.getJSONObject("page");
                // 判断是否为搜索页且包含关键词
                if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                    String item = pageInfo.getString("item");
                    result.put("search_item", item); // 提取搜索关键词
                }
            }
        }

        // 4. 处理操作系统信息(截取版本号前的名称)
        JSONObject deviceInfo = result.getJSONObject("deviceInfo");
        if (deviceInfo != null && deviceInfo.containsKey("os")) {
            // 例如将"Android 10"处理为"Android"
            String os = deviceInfo.getString("os").split(" ")[0];
            deviceInfo.put("os", os);
        }

        return result;
    }
}