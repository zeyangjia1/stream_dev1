package DWD.func;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 年龄计算工具类
 * 功能：将生日字符串转换为具体年龄或年龄区间
 */
public class AgeGroupFunc {
    // 定义日期格式化器，指定日期字符串格式为"yyyy-MM-dd"
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 将生日字符串转换为年龄
     * @param birthday 生日字符串，格式要求：yyyy-MM-dd
     * @return 计算得到的年龄（整数），若输入无效则返回null
     */
    public static Integer getAge(String birthday) {
        // 检查输入是否为空或空字符串
        if (birthday == null || birthday.trim().isEmpty()) {
            return null; // 输入无效时返回null
        }
        try {
            // 将生日字符串解析为LocalDate对象
            LocalDate birthDate = LocalDate.parse(birthday, DATE_FORMATTER);
            // 获取当前日期
            LocalDate currentDate = LocalDate.now();

            // 校验逻辑：生日不能晚于当前日期（防止未来日期）
            if (birthDate.isAfter(currentDate)) {
                return null; // 未来日期视为无效输入
            }

            // 计算精确年龄（基于年、月、日差值）
            return Period.between(birthDate, currentDate).getYears();
        } catch (DateTimeParseException e) {
            // 处理日期格式不符合要求的情况
            System.err.println("生日格式错误: " + birthday + "，正确格式应为 yyyy-MM-dd");
            return null; // 解析失败时返回null
        }
    }

    /**
     * 将生日字符串转换为预定义的年龄区间
     * @param birthday 生日字符串，格式要求：yyyy-MM-dd
     * @return 年龄区间字符串（如"18-24"），若计算失败则返回"未知"
     */
    public static String getAgeRange(String birthday) {
        // 调用getAge方法计算具体年龄
        Integer age = getAge(birthday);
        // 处理年龄计算失败的情况
        if (age == null) {
            return "未知"; // 年龄无效时返回默认值
        }

        // 根据年龄范围返回对应的区间字符串
        if (age >= 18 && age <= 24) {
            return "18-24";
        } else if (age >= 25 && age <= 29) {
            return "25-29";
        } else if (age >= 30 && age <= 34) {
            return "30-34";
        } else if (age >= 35 && age <= 39) {
            return "35-39";
        } else if (age >= 40 && age <= 49) {
            return "40-49";
        } else {
            return "50"; // 50岁及以上统一返回"50"
        }
    }

    // 测试示例（非生产代码，仅用于演示功能）
    public static void main(String[] args) {
        // 示例输入：2005年7月12日出生
        String birthday = "2005-07-12";
        // 输出：年龄: 19 (假设当前年份是2024)
        System.out.println("年龄: " + getAge(birthday));
        // 输出：年龄区间: 18-24岁
        System.out.println("年龄区间: " + getAgeRange(birthday));
    }
}