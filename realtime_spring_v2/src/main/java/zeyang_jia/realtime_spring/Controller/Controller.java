package zeyang_jia.realtime_spring.Controller;

import com.sugar.SugarUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import zeyang_jia.realtime_spring.bean.*;
import zeyang_jia.realtime_spring.server.SugarServer;
import java.util.List;
import java.util.Map;

/**
 * @Package zeyang_jia.Controller.Controller
 * @Author ayang
 * @Date 2025/4/16 21:45
 * @description:
 */
@RestController
public class Controller {
    @Autowired
    SugarServer server;
    @RequestMapping("test1")
    public Map a(){
        List<IS_new> table1 = server.show1();
        return SugarUtil.getPieData(table1);
    }


    @RequestMapping("test2")
    public Map b (){
        List<ch_uv> ch_uvs = server.show2();
        return SugarUtil.getBarData(ch_uvs);

    }



    @RequestMapping("test3")
    public Map c (){
        List<ch_order_count> ch_uvs = server.show3();
        return SugarUtil.getBarData(ch_uvs);
    }

    @RequestMapping("test4")
    public Map d (){
        List<jx> ch_uvs = server.show4();
        return SugarUtil.getMapData(ch_uvs);
    }


    @RequestMapping("test5")
    public Map e (){
        List<kord_count> ch_uvs = server.show5();
        return SugarUtil.getPieData(ch_uvs);

    }
    @RequestMapping("test6")
    public Map f (){
        List<sku_zb> ch_uvs = server.show6();
        return SugarUtil.getBarData(ch_uvs);

    }
    @RequestMapping("baidu")
    public String g (){
        String s="小郭你这头...................";
        return s;

    }

//    INSERT INTO user (username, password, role, status)
//    VALUES ('admin', 'admin', 'ADMIN', 1);
//    source /opt/soft/streampark/script/schema/mysql-schema.sql
//    source /opt/soft/streampark/script/data/mysql-data.sql
     //接口 sm   @map @SElect
    //接口 server @Au
    // 启动类 @ stringbootApp
    // con @ RestController   @RequestMapping端口 @Autowired 调用

}
