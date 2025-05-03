package zeyang_jia.realtime_spring.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import zeyang_jia.realtime_spring.bean.*;
import zeyang_jia.realtime_spring.mapper.SugarMapper;

import java.util.List;

/**
 * @Package zeyang_jia.server.SugarServerImpl
 * @Author ayang
 * @Date 2025/4/16 22:19
 * @description:
 */
@Service
public class SugarServerImpl implements SugarServer {
    @Autowired
    private SugarMapper sugarMapper;
    @Override
    public List<IS_new> show1() {
        return sugarMapper.table1();
    }

    @Override
    public List<ch_uv> show2() {
        return sugarMapper.table2();
    }

    @Override
    public List<ch_order_count> show3() {
        return sugarMapper.table3();
    }

    @Override
    public List<jx> show4() {
        return sugarMapper.table4();
    }

    @Override
    public List<kord_count> show5() {
        return sugarMapper.table5();

    }

    @Override
    public List<sku_zb> show6() {
        return  sugarMapper.table6();

    }


}
