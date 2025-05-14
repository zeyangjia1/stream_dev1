package DWD.func;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class AgeGroupFunc {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /**
     * 将生日字符串转换为年龄
     * @param birthday 生日字符串，格式：yyyy-MM-dd
     * @return 年龄，如果日期无效则返回 null
     */
    public static Integer getAge(String birthday) {
        if (birthday == null || birthday.trim().isEmpty()) {
            return null;
        }
        try {
            LocalDate birthDate = LocalDate.parse(birthday, DATE_FORMATTER);
            LocalDate currentDate = LocalDate.now();

            // 检查生日是否在当前日期之后（防止未来日期导致的异常）
            if (birthDate.isAfter(currentDate)) {
                return null;
            }

            // 计算精确年龄
            return Period.between(birthDate, currentDate).getYears();
        } catch (DateTimeParseException e) {
            // 处理日期格式错误
            System.err.println("生日格式错误: " + birthday + "，正确格式应为 yyyy-MM-dd");
            return null;
        }
    }

    /**
     * 将生日字符串转换为年龄区间
     * @param birthday 生日字符串，格式：yyyy-MM-dd
     * @return 年龄区间字符串，如 "18-24岁"
     */
    public static String getAgeRange(String birthday) {
        Integer age = getAge(birthday);
        if (age == null) {
            return "未知";
        }

        if (age >= 18 && age <= 24) {
            return "18-24岁";
        } else if (age >= 25 && age <= 29) {
            return "25-29岁";
        } else if (age >= 30 && age <= 34) {
            return "30-34岁";
        } else if (age >= 35 && age <= 39) {
            return "35-39岁";
        } else if (age >= 40 && age <= 49) {
            return "40-49岁";
        } else {
            return "50岁以上";
        }
    }

    // 示例用法
    public static void main(String[] args) {
        String birthday = "2005-07-12";
        System.out.println("年龄: " + getAge(birthday)); // 输出: 年龄: 19 (假设当前年份是2024)
        System.out.println("年龄区间: " + getAgeRange(birthday)); // 输出: 年龄区间: 18-24岁
    }
}    