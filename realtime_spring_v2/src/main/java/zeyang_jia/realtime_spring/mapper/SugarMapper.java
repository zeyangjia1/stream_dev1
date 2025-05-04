package zeyang_jia.realtime_spring.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import zeyang_jia.realtime_spring.bean.*;

import java.util.List;

/**
 * @Package zeyang_jia.SugarMapper.SugarMapper
 * @Author ayang
 * @Date 2025/4/16 21:46
 * @description: a
 */
@Mapper
public interface SugarMapper {
    @Select("select vc as name, sum(uvCt)as " +
            "value " +
            "from realtime_v1.dws_traffic_vc_ch_ar_is_new_page_view_window group by vc")
    List<IS_new> table1();
    @Select("select sum(uvCt)as value,ch as name ,0 as  series  from realtime_v1.dws_traffic_vc_ch_ar_is_new_page_view_window \n" +
            "                                    group by ch;")
    List<ch_uv> table2();
    @Select("select sum(orderCount) as value ,provinceName as name,0 as  series\n" +
            "from dws_trade_province_order_window group by provinceName;")
    List<ch_order_count> table3();
    @Select("select sum(orderAmount) as value ,provinceName as name\n" +
            "from dws_trade_province_order_window group by provinceName;")
    List<jx> table4();

    @Select("select keyword as name ,sum(keyword_count) as vlues from\n" +
            "    dws_traffic_source_keyword_page_view_window group by keyword;")
    List<kord_count> table5();
    @Select("select trademarkName as name,sum(orderAmount) as value from\n" +
            "      dws_trade_sku_order_window group by trademarkName;\n")
    List<sku_zb>table6();

}

