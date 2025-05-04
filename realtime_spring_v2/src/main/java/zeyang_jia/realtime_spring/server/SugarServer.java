package zeyang_jia.realtime_spring.server;


import zeyang_jia.realtime_spring.bean.*;

import java.util.List;

/**
 * @Package zeyang_jia.server.SugarServer
 * @Author ayang
 * @Date 2025/4/16 22:19
 * @description:
 */
public interface SugarServer {
    List<IS_new>  show1();
    List<ch_uv>  show2();
    List<ch_order_count>  show3();
    List<jx> show4();
    List<kord_count> show5();
    List<sku_zb> show6();

}
