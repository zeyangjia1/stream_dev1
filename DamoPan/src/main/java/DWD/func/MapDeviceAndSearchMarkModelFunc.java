package DWD.func;

import Bean.DimBaseCategory;
import Utils.JdbcUtils;
import com.alibaba.fastjson.JSONObject;
import com.stream.domain.DimCategoryCompare;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.func.MapDeviceMarkModel
 * @Author zeyang_jia
 * @Date 2025/5/13 21:34
 * @description: 设备与搜索行为打分模型
 * 功能说明：
 * 1. 根据用户设备操作系统(iOS/Android)为不同年龄段分配设备权重分数
 * 2. 根据用户搜索内容匹配商品分类，再根据分类类型为不同年龄段分配搜索权重分数
 * 3. 最终输出包含各年龄段设备和搜索分数的JSON对象
 */
public class MapDeviceAndSearchMarkModelFunc extends RichMapFunction<JSONObject, JSONObject> {

    // 设备类型权重系数
    private final double deviceRate;
    // 搜索行为权重系数
    private final double searchRate;

    // 商品分类映射表，用于快速查找商品所属类别
    private final Map<String, DimBaseCategory> categoryMap;
    // 分类比较规则列表，用于匹配搜索类别与年龄组的关系
    private List<DimCategoryCompare> dimCategoryCompares;

    // 数据库连接对象
    private Connection connection;

    /**
     * 构造函数
     * @param dimBaseCategories 商品分类列表
     * @param deviceRate 设备权重系数
     * @param searchRate 搜索权重系数
     */
    public MapDeviceAndSearchMarkModelFunc(List<DimBaseCategory> dimBaseCategories, double deviceRate, double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
        this.categoryMap = new HashMap<>();

        // 将商品分类数据存入Map，以便快速查询
        for (DimBaseCategory category : dimBaseCategories) {
            categoryMap.put(category.getB3name(), category);
        }
    }

    /**
     * 初始化方法，在函数实例化时调用
     * 用于建立数据库连接并加载分类比较规则
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 建立MySQL数据库连接
        connection = JdbcUtils.getMySQLConnection(
                "jdbc:mysql://cdh03:3306/realtime_v2?useSSL=false",
                "root",
                "root"
        );

        // 查询分类比较规则数据
        String sql = "select id, category_name, search_category from realtime_v2.category_compare_dic";
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);

        super.open(parameters);
    }

    /**
     * 核心处理方法，对每个输入的JSON对象进行打分处理
     * @param jsonObject 输入的用户行为数据
     * @return 包含各年龄段设备和搜索分数的JSON对象
     */
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // ==================== 设备类型分析与打分 ====================
        // 从JSON中提取操作系统信息（可能包含多个标签，如"iOS,Appstore"）
        String os = jsonObject.getString("os");
        String[] labels = os.split(",");
        String judge_os = labels[0]; // 取第一个标签作为主要操作系统

        // 添加判断出的操作系统信息到JSON
        jsonObject.put("judge_os", judge_os);

        // 根据操作系统类型为不同年龄段分配设备分数（权重 * 系数）
        if (judge_os.equals("iOS")) {
            jsonObject.put("device_18-24", round(0.7 * deviceRate));
            jsonObject.put("device_25-29", round(0.6 * deviceRate));
            jsonObject.put("device_30-34", round(0.5 * deviceRate));
            jsonObject.put("device_35-39", round(0.4 * deviceRate));
            jsonObject.put("device_40-49", round(0.3 * deviceRate));
            jsonObject.put("device_50",    round(0.2 * deviceRate));
        } else if (judge_os.equals("Android")) {
            jsonObject.put("device_18-24", round(0.8 * deviceRate));
            jsonObject.put("device_25-29", round(0.7 * deviceRate));
            jsonObject.put("device_30-34", round(0.6 * deviceRate));
            jsonObject.put("device_35-39", round(0.5 * deviceRate));
            jsonObject.put("device_40-49", round(0.4 * deviceRate));
            jsonObject.put("device_50",    round(0.3 * deviceRate));
        }

        // ==================== 搜索内容分析与打分 ====================
        // 从JSON中提取搜索内容
        String searchItem = jsonObject.getString("search_item");

        if (searchItem != null && !searchItem.isEmpty()) {
            // 查找搜索内容对应的商品分类
            DimBaseCategory category = categoryMap.get(searchItem);
            if (category != null) {
                // 将一级分类信息添加到JSON
                jsonObject.put("b1_category", category.getB1name());
            }
        }

        // 根据一级分类查找对应的搜索类别
        String b1Category = jsonObject.getString("b1_category");
        if (b1Category != null && !b1Category.isEmpty()) {
            for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                if (b1Category.equals(dimCategoryCompare.getCategoryName())) {
                    // 将匹配到的搜索类别添加到JSON
                    jsonObject.put("searchCategory", dimCategoryCompare.getSearchCategory());
                    break;
                }
            }
        }

        // 如果没有找到匹配的搜索类别，默认为"unknown"
        String searchCategory = jsonObject.getString("searchCategory");
        if (searchCategory == null) {
            searchCategory = "unknown";
        }

        // 根据搜索类别为不同年龄段分配搜索分数（权重 * 系数）
        switch (searchCategory) {
            case "时尚与潮流":
                // 时尚类搜索：年轻人权重高，年长者权重低
                jsonObject.put("search_18-24", round(0.9 * searchRate));
                jsonObject.put("search_25-29", round(0.7 * searchRate));
                jsonObject.put("search_30-34", round(0.5 * searchRate));
                jsonObject.put("search_35-39", round(0.3 * searchRate));
                jsonObject.put("search_40-49", round(0.2 * searchRate));
                jsonObject.put("search_50",    round(0.1 * searchRate));
                break;
            case "性价比":
                // 性价比类搜索：中年人权重高，年轻人和年长者权重低
                jsonObject.put("search_18-24", round(0.2 * searchRate));
                jsonObject.put("search_25-29", round(0.4 * searchRate));
                jsonObject.put("search_30-34", round(0.6 * searchRate));
                jsonObject.put("search_35-39", round(0.7 * searchRate));
                jsonObject.put("search_40-49", round(0.8 * searchRate));
                jsonObject.put("search_50",    round(0.8 * searchRate));
                break;
            case "健康与养生":
            case "家庭与育儿":
                // 健康/育儿类搜索：中年和年长者权重高，年轻人权重低
                jsonObject.put("search_18-24", round(0.1 * searchRate));
                jsonObject.put("search_25-29", round(0.2 * searchRate));
                jsonObject.put("search_30-34", round(0.4 * searchRate));
                jsonObject.put("search_35-39", round(0.6 * searchRate));
                jsonObject.put("search_40-49", round(0.8 * searchRate));
                jsonObject.put("search_50",    round(0.7 * searchRate));
                break;
            case "科技与数码":
                // 科技数码类搜索：年轻人权重高，年长者权重低
                jsonObject.put("search_18-24", round(0.8 * searchRate));
                jsonObject.put("search_25-29", round(0.6 * searchRate));
                jsonObject.put("search_30-34", round(0.4 * searchRate));
                jsonObject.put("search_35-39", round(0.3 * searchRate));
                jsonObject.put("search_40-49", round(0.2 * searchRate));
                jsonObject.put("search_50",    round(0.1 * searchRate));
                break;
            case "学习与发展":
                // 学习发展类搜索：中青年权重高，年长者权重低
                jsonObject.put("search_18-24", round(0.4 * searchRate));
                jsonObject.put("search_25-29", round(0.5 * searchRate));
                jsonObject.put("search_30-34", round(0.6 * searchRate));
                jsonObject.put("search_35-39", round(0.7 * searchRate));
                jsonObject.put("search_40-49", round(0.8 * searchRate));
                jsonObject.put("search_50",    round(0.7 * searchRate));
                break;
            default:
                // 未知类别：所有年龄段分数为0
                jsonObject.put("search_18-24", 0);
                jsonObject.put("search_25-29", 0);
                jsonObject.put("search_30-34", 0);
                jsonObject.put("search_35-39", 0);
                jsonObject.put("search_40-49", 0);
                jsonObject.put("search_50", 0);
        }

        return jsonObject;
    }

    /**
     * 四舍五入方法，保留3位小数
     * @param value 原始值
     * @return 四舍五入后的值
     */
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    /**
     * 清理方法，在函数关闭时调用
     * 用于关闭数据库连接
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}