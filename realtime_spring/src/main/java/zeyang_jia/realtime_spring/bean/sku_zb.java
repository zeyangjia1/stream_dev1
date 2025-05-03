package zeyang_jia.realtime_spring.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package zeyang_jia.realtime_spring.bean.sku_zb
 * @Author ayang
 * @Date 2025/4/17 14:06
 * @description: 各品牌交易额
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class sku_zb {
    private  String name;
    private Double value;

}
