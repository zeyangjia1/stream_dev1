package zeyang_jia.realtime_spring.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package zeyang_jia.realtime_spring.bean.ch_order_count
 * @Author ayang
 * @Date 2025/4/17 13:45
 * @description:各个品牌交易额实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder

public class ch_order_count {
    private  String name;
    private int value;

}
