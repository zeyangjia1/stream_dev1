package SensitiveWordFiltering;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import utils.HbaseUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Flink异步HBase维表查询函数，用于敏感词过滤场景
 * 功能：根据输入的评价内容，从HBase中异步查询对应的字典名称(dic_name)
 * 特性：
 *   1. 使用Guava Cache缓存高频访问的HBase记录，减少IO压力
 *   2. 基于MD5哈希生成RowKey，确保数据均匀分布
 *   3. 异步查询机制，避免阻塞Flink流处理
 */
public class AsyncHbaseDimBaseDicFunc extends RichAsyncFunction<JSONObject,JSONObject> {

    private transient Connection hbaseConn;       // HBase连接对象
    private transient Table dimTable;           // 维度表操作对象
    // 本地缓存：RowKey -> dic_name，减少重复HBase查询
    private transient Cache<String, String> cache;

    /**
     * 函数初始化方法
     * 1. 建立HBase连接并获取表对象
     * 2. 初始化LRU缓存(最大1000条记录，10分钟过期)
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 连接HBase集群，使用ZooKeeper地址
        hbaseConn = new HbaseUtils("cdh01:2181,cdh02:2181,cdh03:2181").getConnection();
        dimTable = hbaseConn.getTable(TableName.valueOf("gmall_config:dim_base_dic"));

        // 配置本地缓存策略：
        // - 最大容量1000，防止内存溢出
        // - 写入后10分钟过期，保证数据时效性
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        super.open(parameters);
    }

    /**
     * 异步查询核心方法
     * 1. 从输入JSON中提取评价内容
     * 2. 生成MD5 RowKey并检查本地缓存
     * 3. 缓存命中则直接返回，未命中则异步查询HBase
     * 4. 查询结果存入缓存并输出
     */
    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        // 从Flink流中获取评价内容
        String appraise = input.getJSONObject("after").getString("appraise");

        // 生成MD5 RowKey，确保数据在HBase中均匀分布
        String rowKey = MD5Hash.getMD5AsHex(appraise.getBytes(StandardCharsets.UTF_8));

        // 优先从本地缓存获取结果，减少HBase访问
        String cachedDicName = cache.getIfPresent(rowKey);
        if (cachedDicName != null) {
            enrichAndEmit(input, cachedDicName, resultFuture);
            return;
        }

        // 异步查询HBase
        CompletableFuture.supplyAsync(() -> {
            Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
            try {
                // 执行HBase查询
                Result result = dimTable.get(get);
                if (result.isEmpty()) {
                    return null; // 未找到记录
                }
                // 从结果中提取字典名称(dic_name)
                return Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dic_name")));
            } catch (IOException e) {
                // 异常处理：抛出运行时异常，包含定位信息
                throw new RuntimeException("Class: AsyncHbaseDimBaseDicFunc Line 66 HBase query failed ! ! !",e);
            }
        }).thenAccept(dicName -> {
            // 查询完成后的回调处理
            if (dicName != null) {
                // 缓存新结果并输出
                cache.put(rowKey, dicName);
                enrichAndEmit(input, dicName, resultFuture);
            } else {
                // 未找到记录，使用默认值
                enrichAndEmit(input, "N/A", resultFuture);
            }
        });
    }

    /**
     * 数据增强并输出结果
     * 将查询到的字典名称添加到原始JSON中，并发送给下游
     */
    private void enrichAndEmit(JSONObject input, String dicName, ResultFuture<JSONObject> resultFuture) {
        JSONObject after = input.getJSONObject("after");
        after.put("dic_name", dicName); // 添加字典名称字段
        resultFuture.complete(Collections.singleton(input)); // 输出结果
    }

    /**
     * 超时处理方法
     * 当前实现仅调用父类方法，可扩展为自定义超时策略
     */
    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }

    /**
     * 资源释放方法
     * 确保HBase连接和表对象正确关闭，防止资源泄漏
     */
    @Override
    public void close() throws Exception {
        try {
            if (dimTable != null) dimTable.close();
            if (hbaseConn != null) hbaseConn.close();
        } catch (Exception e) {
            e.printStackTrace(); // 记录关闭异常
        }
        super.close();
    }
}