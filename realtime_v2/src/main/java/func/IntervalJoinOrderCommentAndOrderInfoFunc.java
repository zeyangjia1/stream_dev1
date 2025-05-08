package func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package func.IntervalJoinOrderCommentAndOrderInfoFunc
 * @Author zeyang_jia
 * @Date 2025/5/7 15:40
 * @description: orderComment Join orderInfo Msg
 */
public class IntervalJoinOrderCommentAndOrderInfoFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out){
        JSONObject enrichedComment = (JSONObject)comment.clone();

        for (String key : info.keySet()) {
            enrichedComment.put("info_" + key, info.get(key));
        }
        out.collect(enrichedComment);
    }
}
