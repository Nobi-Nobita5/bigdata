package com.atguigu.gmall.publisherrealtime.controller;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 控制层
 */
@RestController
//包括ResponseBoy和Controller
public class PublisherContoller {



    @Autowired
    PublisherService publisherService ;

    /**
     *
     * 交易分析 - 明细
     * http://bigdata.gmall.com/detailByItem?date=2021-02-02&itemName=小米手机&pageNo=1&pageSize=20
     *
     * [
     * { total: 24},
     * { detail: Object}
     * ]
     * 详细一条订单数据封装在Object中
     * 返回Map<String, Object>
     */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date ,
                                            @RequestParam("itemName") String itemName ,
                                            @RequestParam(value ="pageNo" , required = false  , defaultValue = "1") Integer pageNo ,
                                            @RequestParam(value = "pageSize" , required = false , defaultValue = "20") Integer pageSize){
        Map<String, Object> results =  publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
        return results ;
    }


    /**
     * 交易分析 - 按照类别(年龄、性别)统计
     *
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     * [
     * { value: 1048, name: "男" },
     * { value: 735, name: "女" }
     * ]
     *
     * [
     * { value: 1048, name: "20 岁以下" },
     * { value: 735, name: "20 岁至 29 岁" } ,
     * { value: 34, name: "30 岁以上" }
     * ]
     *
     * 封装NameValue对象，返回List<NameValue>的Json串
     */
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(
            @RequestParam("itemName")String itemName ,
            @RequestParam("date") String date ,
            @RequestParam("t") String t){
        List<NameValue>  results =  publisherService.doStatsByItem(itemName , date , t );
        return results;
    }



    /**
     * 日活分析
     * @param td
     * @return
     * { dauTotal:123,
     *  dauYd:{"12":90,"13":33,"17":166 },
     *  dauTd:{"11":232,"15":45,"18":76}
     * }
     * Map<String, Object>类型的返回值满足上述要求
     * 加上responseBody，Map自动转换为json数据
     * 也可加在类定义上
     */
    @GetMapping("dauRealtime")
    public Map<String, Object>  dauRealtime(@RequestParam("td") String td  ){

        Map<String, Object>  results = publisherService.doDauRealtime(td) ;

        return results ;
    }

}
