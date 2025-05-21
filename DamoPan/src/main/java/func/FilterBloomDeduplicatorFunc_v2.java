package func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @Package com.retailersv1.func.FilterBloomDeduplicatorFunc
 * @Author zeyang_jia
 * @Date 2025/5/12
 * @description: 布隆过滤器
 */
public class FilterBloomDeduplicatorFunc_v2 extends RichFilterFunction<JSONObject> {

    // 定义日志记录器
    private static final Logger logger = LoggerFactory.getLogger(FilterBloomDeduplicatorFunc_v2.class);

    // 预期插入的元素数量
    private final int expectedInsertions;
    // 误判率
    private final double falsePositiveRate;
    // 定义布隆过滤器的状态
    private transient ValueState<byte[]> bloomState;

    // 构造函数，初始化预期插入元素数量和误判率
    public FilterBloomDeduplicatorFunc_v2(int expectedInsertions, double falsePositiveRate) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
    }

    // 重写open方法，初始化状态描述符并获取状态
    @Override
    public void open(Configuration parameters) {
        // 创建状态描述符，指定状态名称和序列化器
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState",
                BytePrimitiveArraySerializer.INSTANCE
        );

        // 获取运行时上下文的状态
        bloomState = getRuntimeContext().getState(descriptor);
    }

    // 重写filter方法，对输入的JSONObject进行过滤
    @Override
    public boolean filter(JSONObject value) throws Exception {
        // 从JSONObject中获取订单ID
        long orderId = value.getJSONObject("after").getLong("id");
        // 从JSONObject中获取时间戳
        long tsMs = value.getLong("ts_ms");
        // 组合订单ID和时间戳作为复合键
        String compositeKey = orderId + "_" + tsMs;

        // 读取状态
        byte[] bitArray = bloomState.value();
        // 如果状态为空，初始化位阵列
        if (bitArray == null) {
            bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
        }

        // 初始化可能包含标志
        boolean mightContain = true;
        // 计算复合键的第一个哈希值
        int hash1 = hash(compositeKey);
        // 计算复合键的第二个哈希值
        int hash2 = hash1 >>> 16;

        // 计算最优哈希函数数量
        for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
            // 计算组合哈希值
            int combinedHash = hash1 + (i * hash2);
            // 如果组合哈希值为负数，取其补码
            if (combinedHash < 0) combinedHash = ~combinedHash;
            // 计算位阵列中的位置
            int pos = combinedHash % (bitArray.length * 8);

            // 计算字节位置和位位置
            int bytePos = pos / 8;
            int bitPos = pos % 8;
            // 获取当前字节
            byte current = bitArray[bytePos];

            // 如果当前位为0，说明可能不包含，更新位阵列并设置标志
            if ((current & (1 << bitPos)) == 0) {
                mightContain = false;
                bitArray[bytePos] = (byte) (current | (1 << bitPos));
            }
        }

        // 如果是新数据，更新状态并保留
        if (!mightContain) {
            bloomState.update(bitArray);
            return true;
        }

        // 可能重复的数据，过滤并记录日志
        logger.warn("check duplicate data : {}", value);
        return false;
    }

    // 计算最优哈希函数数量的方法
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    // 计算最优位数的方法
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    // 计算哈希值的方法
    private int hash(String key) {
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }
}