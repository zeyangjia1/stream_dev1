package realtime_Dwd;

import Base.BasesqlApp;
import constat.constat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.Sqlutil;

/**
 * @Package realtime_Dwd.Add_the_fact_sheet
 * @Author a_yang
 * @Date 2025/4/11 14:49
 * @description: 加购事实表
 */

public class Addthefactsheet extends BasesqlApp {
    public static void main(String[] args) {
        new Addthefactsheet().start(10003,4,"dwd_Addthefactsheet");
    }

    @Override
public void handle(StreamTableEnvironment tableEnv) {
    // 读取ODS层的Kafka数据，topic名称为TOPIC_DWD_TRADE_CART_ADD
    readOdsDb(tableEnv, constat.TOPIC_DWD_TRADE_CART_ADD);

    // 使用SQL查询处理购物车信息表的数据
    Table cartInfo = tableEnv.sqlQuery("select \n" +
            "   `after`['id'] id,\n" + // 提取新增或更新后的id字段
            "   `after`['user_id'] user_id,\n" + // 提取用户ID
            "   `after`['sku_id'] sku_id,\n" + // 提取SKU商品ID
            "   if(op='c',`after`['sku_num'], " + // 如果是新增操作，则直接使用新的sku_num
            "CAST((CAST(after['sku_num'] AS INT) " +
            "- CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
            // 如果是更新操作且库存增加，则计算差值作为加购数量
            "   ts_ms\n" + // 提取时间戳
            "from topic_table_v1 \n" + // 数据来源表
            "where source['table']='cart_info' \n" + // 过滤购物车信息表
            "and (\n" +
            "    op = 'c'\n" + // 操作类型为新增
            "    or\n" +
            "    (op='u' and `before`['sku_num'] is not null " + // 或者更新操作并且之前的库存数不为空
            "and (CAST(after['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT)))\n" +
            // 并且更新后的库存大于更新前的库存
            ")");

    // 执行查询并打印结果到控制台（用于调试）
    cartInfo.execute().print();

    // 创建目标Kafka结果表，用于写入处理结果
    tableEnv.executeSql(" create table " + constat.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
            "    id string,\n" + // 购物车记录ID
            "    user_id string,\n" + // 用户ID
            "    sku_id string,\n" + // SKU商品ID
            "    sku_num string,\n" + // 加购数量
            "    ts bigint,\n" + // 时间戳
            "    PRIMARY KEY (id) NOT ENFORCED\n" + // 主键约束（未强制执行）
            " ) " + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_CART_ADD)); // 使用工具类生成Upsert Kafka DDL语句

    // 注释掉的代码：将处理后的结果写入Kafka主题
    // cartInfo.executeInsert(constat.TOPIC_DWD_TRADE_CART_ADD);
}

}
