package DWD.func;

/**
 * @Package DWD.func.TimePeriodFunc
 * @Author zeyang_jia
 * @Date 2025/5/14 11:13
 * @description: 转换时间 例如 : 凌晨 下午
 */
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;

public class TimePeriodFunc {

    public static String getPriceRange(double totalAmount) {
        if (totalAmount < 0) {
            throw new IllegalArgumentException("价格不能为负数: " + totalAmount);
        }

        if (totalAmount < 1000) {
            return "低价商品";
        } else if (totalAmount <= 4000) {
            return "中间商品";
        } else {
            return "高价商品";
        }
    }

    public static String getTimePeriod(long createTimeMillis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createTimeMillis);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        if (hour >= 0 && hour < 5) {
            return "凌晨";
        } else if (hour >= 5 && hour < 8) {
            return "早晨";
        } else if (hour >= 8 && hour < 12) {
            return "上午";
        } else if (hour >= 12 && hour < 14) {
            return "中午";
        } else if (hour >= 14 && hour < 18) {
            return "下午";
        } else if (hour >= 18 && hour < 21) {
            return "晚上";
        } else {
            return "夜间";
        }
    }
    public static String getConstellation(String birthdayStr) {
        // 将生日字符串解析为LocalDate对象
        LocalDate birthday = LocalDate.parse(birthdayStr);
        int month = birthday.getMonthValue();
        int day = birthday.getDayOfMonth();

        // 根据月份和日期判断星座
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        } else {
            return "无效日期";
        }
    }

    public static String getEra(String birthdayStr) {
        try {
            // 解析生日字符串为年份
            LocalDate date = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
            int year = date.getYear();

            // 处理1900年以前的年份（可选）
            if (year < 1950) {
                return "早于50年代";
            }

            // 计算年代（取年份的十位和个位，注意2000-2009为"00年代"）
            int decadeValue = (year % 100) / 10; // 提取十位数字（如2023 → 23 → 2）
            String decadeSuffix;

            if (year >= 2000) { // 2000年后的特殊处理（2000-2009为"00年代"）
                decadeSuffix = (decadeValue == 0) ? "00" : decadeValue + "0";
            } else { // 1950-1999年（如1953 → 50年代）
                decadeSuffix = decadeValue + "0";
            }

            return decadeSuffix + "年代";
        } catch (Exception e) {
            return "未知"; // 处理无效日期格式
        }
    }





    // 测试方法
    public static void main(String[] args) {
//        //使用当前时间去测试
//        long createTimeMillis = System.currentTimeMillis();
//        String timePeriod = getTimePeriod(createTimeMillis);
//        System.out.println(timePeriod);
        //测试年代
//        String[] birthdays = {
//                "1955-03-12",  // 50年代
//                "1968-12-25",  // 60年代
//                "1999-01-01",  // 90年代
//                "2000-12-31",  // 00年代
//                "2015-05-18",  // 10年代
//                "2023-10-01",  // 20年代
//        };
//
//        for (String birthday : birthdays) {
//            String decade = getEra(birthday);
//            System.out.println(birthday + " → " + decade);
//        }
        //测试星座
//        String birthday = "2000-05-20";
//        String constellation = getConstellation(birthday);
//        System.out.println(constellation);

    }
}
