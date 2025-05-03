package zeyang_jia.realtime_spring.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package zeyang_jia.realtime_spring.bean.ch_uv
 * @Author ayang
 * @Date 2025/4/17 10:30
 * @description: 各渠道独立访客数实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ch_uv {
    private  String name;
    private int value;

}
