package Utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL数据库操作工具类，提供数据库连接、关闭和查询功能
 * 支持将数据库字段从下划线格式自动转换为驼峰格式
 */
public class JdbcUtils {

    /**
     * 获取MySQL数据库连接
     * @param mysqlUrl 数据库连接URL
     * @param username 用户名
     * @param pwd 密码
     * @return 数据库连接对象
     * @throws Exception 可能抛出ClassNotFoundException或SQLException
     */
    public static Connection getMySQLConnection(String mysqlUrl, String username, String pwd) throws Exception {
        // 加载MySQL JDBC驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 通过DriverManager获取数据库连接
        return DriverManager.getConnection(mysqlUrl, username, pwd);
    }

    /**
     * 关闭MySQL数据库连接
     * @param conn 数据库连接对象
     * @throws SQLException 关闭连接失败时抛出
     */
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        // 检查连接是否有效且未关闭
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    /**
     * 执行SQL查询并返回对象列表（使用列名）
     * @param conn 数据库连接
     * @param sql SQL查询语句
     * @param clz 结果对象类型
     * @param isUnderlineToCamel 是否将下划线命名转换为驼峰命名（可选参数，默认false）
     * @param <T> 泛型类型
     * @return 对象列表
     * @throws Exception 反射创建对象或SQL执行异常
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        // 设置默认转换规则（下划线转驼峰）
        boolean defaultIsUToC = false;

        // 处理可选参数
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        // 预编译SQL语句
        PreparedStatement ps = conn.prepareStatement(sql);
        // 执行查询获取结果集
        ResultSet rs = ps.executeQuery();
        // 获取结果集元数据
        ResultSetMetaData metaData = rs.getMetaData();

        // 遍历结果集
        while (rs.next()) {
            // 通过反射创建对象实例
            T obj = clz.newInstance();

            // 处理每行的所有列
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名
                String columnName = metaData.getColumnName(i);
                // 获取列值
                Object columnValue = rs.getObject(i);

                // 进行列名格式转换（如果需要）
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 使用BeanUtils设置对象属性值
                BeanUtils.setProperty(obj, columnName, columnValue);
            }

            // 将处理好的对象添加到结果列表
            resList.add(obj);
        }

        return resList;
    }

    /**
     * 执行SQL查询并返回对象列表（使用列标签）
     * @param conn 数据库连接
     * @param sql SQL查询语句
     * @param clz 结果对象类型
     * @param isUnderlineToCamel 是否将下划线命名转换为驼峰命名（可选参数，默认false）
     * @param <T> 泛型类型
     * @return 对象列表
     * @throws Exception 反射创建对象或SQL执行异常
     */
    public static <T> List<T> queryList2(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        // 设置默认转换规则
        boolean defaultIsUToC = false;

        // 处理可选参数
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        // 预编译SQL语句
        PreparedStatement ps = conn.prepareStatement(sql);
        // 执行查询获取结果集
        ResultSet rs = ps.executeQuery();
        // 获取结果集元数据
        ResultSetMetaData metaData = rs.getMetaData();

        // 遍历结果集
        while (rs.next()) {
            // 通过反射创建对象实例
            T obj = clz.newInstance();

            // 处理每行的所有列
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列标签（优先使用SQL中定义的别名）
                String columnName = metaData.getColumnLabel(i);
                // 获取列值
                Object columnValue = rs.getObject(i);

                // 进行列名格式转换（如果需要）
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 使用BeanUtils设置对象属性值
                BeanUtils.setProperty(obj, columnName, columnValue);
            }

            // 将处理好的对象添加到结果列表
            resList.add(obj);
        }

        return resList;
    }
}