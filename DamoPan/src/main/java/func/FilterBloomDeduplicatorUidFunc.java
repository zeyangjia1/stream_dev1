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
 * @description: 基于布隆过滤器的数据流去重功能
 * 核心功能：通过布隆过滤器实现大规模数据的近似去重，避免重复处理相同数据
 */
public class FilterBloomDeduplicatorUidFunc extends RichFilterFunction<JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(FilterBloomDeduplicatorUidFunc.class);

    // 预期插入元素数量
    private final int expectedInsertions;
    // 布隆过滤器的误判率
    private final double falsePositiveRate;
    // 用于去重的用户ID字段名
    private final String uid;
    // 时间戳字段名（用于构建复合键）
    private final String ts_ms;

    // 存储布隆过滤器的位阵列状态
    private transient ValueState<byte[]> bloomState;

    /**
     * 构造函数
     * @param expectedInsertions 预期插入的元素数量
     * @param falsePositiveRate 允许的误判率
     * @param uid 用户ID字段名
     * @param ts_ms 时间戳字段名
     */
    public FilterBloomDeduplicatorUidFunc(int expectedInsertions, double falsePositiveRate, String uid, String ts_ms) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
        this.uid = uid;
        this.ts_ms = ts_ms;
    }

    /**
     * 初始化方法，在函数实例化时调用
     * 用于初始化布隆过滤器的状态
     */
    @Override
    public void open(Configuration parameters) {
        // 创建状态描述符，用于存储布隆过滤器的位阵列
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState", // 状态名称
                BytePrimitiveArraySerializer.INSTANCE // 状态类型：字节数组
        );

        // 获取运行时上下文的状态对象
        bloomState = getRuntimeContext().getState(descriptor);
    }

    /**
     * 核心过滤方法，判断元素是否重复
     * @param value 输入的JSON对象
     * @return true表示元素不重复，保留；false表示可能重复，过滤
     */
    @Override
    public boolean filter(JSONObject value) throws Exception {
        // 从JSON对象中提取用户ID和时间戳
        long orderId = value.getLong(uid);
        long tsMs = value.getLong(ts_ms);

        // 构建复合键（用户ID_时间戳），提高去重精度
        String compositeKey = orderId + "_" + tsMs;

        // 读取当前布隆过滤器的位阵列状态
        byte[] bitArray = bloomState.value();

        // 如果状态为空，初始化位阵列
        if (bitArray == null) {
            // 计算最佳位数组大小，并向上取整到最近的字节
            bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
        }

        // 判断元素是否可能已存在
        boolean mightContain = true;

        // 生成两个基础哈希值
        int hash1 = hash(compositeKey);
        int hash2 = hash1 >>> 16; // 取高16位作为第二个哈希值

        // 根据最佳哈希函数数量生成多个哈希值
        for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
            // 生成组合哈希值
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) combinedHash = ~combinedHash; // 确保为正数

            // 计算位阵列中的位置
            int pos = combinedHash % (bitArray.length * 8);
            int bytePos = pos / 8; // 字节位置
            int bitPos = pos % 8;  // 位位置
            byte current = bitArray[bytePos];

            // 如果对应位为0，表示元素肯定不存在
            if ((current & (1 << bitPos)) == 0) {
                mightContain = false;
                // 更新位阵列，将对应位设为1
                bitArray[bytePos] = (byte) (current | (1 << bitPos));
            }
        }

        // 如果元素不存在，更新状态并保留该元素
        if (!mightContain) {
            bloomState.update(bitArray);
            return true;
        }

        // 元素可能已存在（存在误判可能），过滤该元素
        logger.warn("检测到可能的重复数据: {}", value);
        return false;
    }

    /**
     * 计算最佳哈希函数数量
     * @param n 预期插入元素数量
     * @param m 位数组大小
     * @return 最佳哈希函数数量
     */
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * 计算最佳位数组大小
     * @param n 预期插入元素数量
     * @param p 期望的误判率
     * @return 最佳位数组大小（比特数）
     */
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE; // 避免除零错误
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 生成哈希值
     * @param key 输入键
     * @return 哈希值
     */
    private int hash(String key) {
        // 使用Murmur3_128哈希算法生成哈希值
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }
}