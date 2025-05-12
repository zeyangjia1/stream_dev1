package Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package Bean.CommonTable_Dim
 * @Author zeyang_jia
 * @Date 2025/5/12 10:33
 * @description: dim 层配置维度数据实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class CommonTable_Dim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkRowKey;

    // 配置表操作类型
    String op;
}
