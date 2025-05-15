package DWD.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Package DWD.func.CategoryTmName
 * @Author zeyang_jia
 * @Date 2025/5/15 9:31
 * @description: 给品牌和类目打分
 */
public class CategoryTmName extends RichMapFunction<JSONObject, JSONObject> {

    // 定义品类集合
    private Set<String> trendCategoryValues;
    private Set<String> homeCategoryValues;
    private Set<String> healthCategoryValues;

    // 定义品牌集合
    private Set<String> equipmentTmValues;
    private Set<String> trendTmValues;

    // 定义年龄组映射
    private Map<String, String> ageGroupMapping;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // 初始化集合和映射
        trendCategoryValues = new HashSet<>(Arrays.asList("珠宝", "礼品箱包", "鞋靴", "服饰内衣", "个护化妆", "数码"));
        homeCategoryValues = new HashSet<>(Arrays.asList("母婴", "钟表", "厨具", "电脑办公", "家居家装", "家用电器", "图书、音像、电子书刊", "手机", "汽车用品"));
        healthCategoryValues = new HashSet<>(Arrays.asList("运动健康", "食品饮料、保健食品"));

        equipmentTmValues = new HashSet<>(Arrays.asList("Redmi", "苹果", "联想", "TCL", "小米"));
        trendTmValues = new HashSet<>(Arrays.asList("长粒香", "金沙河", "索芙特", "CAREMiLLE", "欧莱雅", "香奈儿"));

        // 初始化年龄组映射，处理可能的变体
        ageGroupMapping = new HashMap<>();
        ageGroupMapping.put("18-24", "18-24");
        ageGroupMapping.put("18-24", "18-24");
        ageGroupMapping.put("25-29", "25-29");
        ageGroupMapping.put("25-29", "25-29");
        ageGroupMapping.put("30-34", "30-34");
        ageGroupMapping.put("30-34", "30-34");
        ageGroupMapping.put("35-39", "35-39");
        ageGroupMapping.put("35-39", "35-39");
        ageGroupMapping.put("40-49", "40-49");
        ageGroupMapping.put("40-49", "40-49");
        ageGroupMapping.put("50", "50");
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();

        try {
            // 获取字段值
            String tmName = jsonObject.getString("tm_name");
            String categoryName = jsonObject.getString("category_name");
            String ageGroup = jsonObject.getString("age_group");


            if (ageGroup == null || ageGroup.trim().isEmpty()) {
                System.out.println("警告: age_group 为空或 null");
                jsonObject.put("result", result);
                return jsonObject;
            }

            // 标准化年龄组
            ageGroup = ageGroup.trim();
            String normalizedAgeGroup = ageGroupMapping.getOrDefault(ageGroup, null);

            if (normalizedAgeGroup == null) {
                System.out.println("警告: 未知的年龄组格式: " + ageGroup);
                return jsonObject;
            }

            // 根据年龄组计算分数
            switch (normalizedAgeGroup) {
                case "18-24":
                    calculateScoresFor18To24(result, categoryName, tmName);
                    break;
                case "25-29":
                    calculateScoresFor25To29(result, categoryName, tmName);
                    break;
                case "30-34":
                    calculateScoresFor30To34(result, categoryName, tmName);
                    break;
                case "35-39":
                    calculateScoresFor35To39(result, categoryName, tmName);
                    break;
                case "40-49":
                    calculateScoresFor40To49(result, categoryName, tmName);
                    break;
                case "50":
                    calculateScoresFor50Plus(result, categoryName, tmName);
                    break;
            }


        } catch (Exception e) {
            System.err.println("处理记录时发生错误: " + e.getMessage());
            e.printStackTrace();
        }

        jsonObject.put("result2", result);
        return jsonObject;
    }

    // 各个年龄组的计算方法，减少代码重复
    private void calculateScoresFor18To24(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_18_24", round(0.9 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_18_24", round(0.2 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_18_24", round(0.1 * 0.3));
        }

        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_18_24", round(0.9 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_18_24", round(0.1 * 0.2));
        }
    }

    private void calculateScoresFor25To29(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_25_29", round(0.8 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_25_29", round(0.4 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_25_29", round(0.2 * 0.3));
        }

        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_25_29", round(0.7 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_25_29", round(0.3 * 0.2));
        }
    }

    private void calculateScoresFor30To34(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_30_34", round(0.6 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_30_34", round(0.4 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_30_34", round(0.2 * 0.3));
        }

        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_30_34", round(0.5 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_30_34", round(0.5 * 0.2));
        }
    }

    private void calculateScoresFor35To39(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_35_39", round(0.4 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_35_39", round(0.8 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_35_39", round(0.6 * 0.3));
        }

        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_35_39", round(0.3 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_35_39", round(0.7 * 0.2));
        }
    }

    private void calculateScoresFor40To49(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_40_49", round(0.2 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_40_49", round(0.9 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_40_49", round(0.8 * 0.3));
        }
        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_40_49", round(0.2 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_40_49", round(0.8 * 0.2));
        }
    }

    private void calculateScoresFor50Plus(JSONObject result, String categoryName, String tmName) {
        // 判断品类
        if (trendCategoryValues.contains(categoryName)) {
            result.put("cm_50", round(0.1 * 0.3));
        } else if (homeCategoryValues.contains(categoryName)) {
            result.put("cm_50", round(0.7 * 0.3));
        } else if (healthCategoryValues.contains(categoryName)) {
            result.put("cm_50", round(0.9 * 0.3));
        }

        // 判断品牌
        if (equipmentTmValues.contains(tmName)) {
            result.put("tm_50", round(0.1 * 0.2));
        } else if (trendTmValues.contains(tmName)) {
            result.put("tm_50", round(0.9 * 0.2));
        }
    }

    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}