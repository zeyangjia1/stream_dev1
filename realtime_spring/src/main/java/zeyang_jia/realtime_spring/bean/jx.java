package zeyang_jia.realtime_spring.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package zeyang_jia.realtime_spring.bean.jx
 * @Author ayang
 * @Date 2025/4/17 11:49
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class jx {
    private  String name;
    private int value;

}
