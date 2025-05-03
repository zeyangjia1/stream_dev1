package zeyang_jia.realtime_spring.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package zeyang_jia.realtime_spring.bean.kord_count
 * @Author ayang
 * @Date 2025/4/17 13:57
 * @description: 搜索词
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class kord_count {
    private  String name;
    private int value;

}
