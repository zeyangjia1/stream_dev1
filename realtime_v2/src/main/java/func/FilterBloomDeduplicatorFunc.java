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
 * @Package func.FilterBloomDeduplicatorFunc
 * @Author zeyang_jia
 * @Date 2025/5/7 11:01
 * @description: 布隆过滤器
 */
public class FilterBloomDeduplicatorFunc extends RichFilterFunction<JSONObject> {

    // 静态的日志记录器
    private static final Logger logger = LoggerFactory.getLogger(FilterBloomDeduplicatorFunc.class);

    // 预期数量
    private final int expectedInsertions;
    // 误判率
    private final double falsePositiveRate;
    // 阵列的状态
    private transient ValueState<byte[]> bloomState;

    // 构造函数，接收预期插入数量和误判率作为参数
    public FilterBloomDeduplicatorFunc(int expectedInsertions, double falsePositiveRate) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
    }

    // 初始化状态
    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState",
                BytePrimitiveArraySerializer.INSTANCE  // 序列化器
        );

        // 获取运行中状态
        bloomState = getRuntimeContext().getState(descriptor);
    }

    // 过滤函数，也就是看是不是json函数
    @Override
    public boolean filter(JSONObject value) throws Exception {
        long orderId = value.getLong("order_id");
        long tsMs = value.getLong("ts_ms");
        // 复合键
        String compositeKey = orderId + "_" + tsMs;

        byte[] bitArray = bloomState.value();
        // 如果空，则初始化一个新的位阵列
        if (bitArray == null) {
            bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
        }

        // 假设已存在
        boolean mightContain = true;
        // 复合键的第一个哈希值
        int hash1 = hash(compositeKey);
        // 复合键的第二个哈希值
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
            int combinedHash = hash1 + (i * hash2);
            // 如果组合哈希值为负数，则取其补码
            if (combinedHash < 0) combinedHash = ~combinedHash;
            // 计算位阵列中的位置
            int pos = combinedHash % (bitArray.length * 8);
            // 计算字节位置
            int bytePos = pos / 8;
            // 计算位位置
            int bitPos = pos % 8;
            // 获取当前字节
            byte current = bitArray[bytePos];

            // 如果该位为 0，可能不存在
            if ((current & (1 << bitPos)) == 0) {
                mightContain = false;
                // 将该位设置为 1
                bitArray[bytePos] = (byte) (current | (1 << bitPos));
            }
        }
        if (!mightContain) {
            bloomState.update(bitArray);
            return true;
        }
        //如果存在那么就警告并过滤
        logger.warn("check duplicate data : {}", value);
        return false;
    }

    // 最佳数量
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    // 计算布隆过滤器所需的最佳位数
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }
//    MurmurHash3 是一种非加密型哈希函数，适用于快速哈希和需要高吞吐量的场景。
//    它由 Austin Appleby 在 2008 年开发，目前广泛应用于各类数据处理系统和分布式计算框架中。
    // 使用 MurmurHash3 算法计算字符串的哈希值

    private int hash(String key) {
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }



}