/**
  DROP TABLE IF EXISTS tb_order_overall;
CREATE TABLE tb_order_overall (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
    order_id INT NOT NULL COMMENT '订单号',
    uid INT NOT NULL COMMENT '用户ID',
    event_time datetime COMMENT '下单时间',
    total_amount DECIMAL NOT NULL COMMENT '订单总金额',
    total_cnt INT NOT NULL COMMENT '订单商品总件数',
    `status` TINYINT NOT NULL COMMENT '订单状态'
) CHARACTER SET utf8 COLLATE utf8_bin;

INSERT INTO tb_order_overall(order_id, uid, event_time, total_amount, total_cnt, `status`) VALUES
  (301001, 101, '2021-10-01 10:00:00', 30000, 3, 1),
  (301002, 102, '2021-10-01 11:00:00', 23900, 2, 1),
  (301003, 103, '2021-10-02 10:00:00', 31000, 2, 1);

DROP TABLE IF EXISTS tb_product_info;
CREATE TABLE tb_product_info (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
    product_id INT NOT NULL COMMENT '商品ID',
    shop_id INT NOT NULL COMMENT '店铺ID',
    tag VARCHAR(12) COMMENT '商品类别标签',
    in_price DECIMAL NOT NULL COMMENT '进货价格',
    quantity INT NOT NULL COMMENT '进货数量',
    release_time datetime COMMENT '上架时间'
) CHARACTER SET utf8 COLLATE utf8_bin;

DROP TABLE IF EXISTS tb_order_detail;
CREATE TABLE tb_order_detail (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
    order_id INT NOT NULL COMMENT '订单号',
    product_id INT NOT NULL COMMENT '商品ID',
    price DECIMAL NOT NULL COMMENT '商品单价',
    cnt INT NOT NULL COMMENT '下单数量'
) CHARACTER SET utf8 COLLATE utf8_bin;

INSERT INTO tb_product_info(product_id, shop_id, tag, in_price, quantity, release_time) VALUES
  (8001, 901, '家电', 6000, 100, '2020-01-01 10:00:00'),
  (8002, 902, '家电', 12000, 50, '2020-01-01 10:00:00'),
  (8003, 901, '3C数码', 12000, 50, '2020-01-01 10:00:00');

INSERT INTO tb_order_detail(order_id, product_id, price, cnt) VALUES
  (301001, 8001, 8500, 2),
  (301001, 8002, 15000, 1),
  (301002, 8001, 8500, 1),
  (301002, 8002, 16000, 1),
  (301003, 8002, 14000, 1),
  (301003, 8003, 18000, 1);
  ----------------------------------------
场景逻辑说明：
用户将购物车中多件商品一起下单时，订单总表会生成一个订单（但此时未付款，status-订单状态为0表示待付款），在订单明细表生成该订单中每个商品的信息；
当用户支付完成时，在订单总表修改对应订单记录的status-订单状态为1表示已付款；
若用户退货退款，在订单总表生成一条交易总金额为负值的记录（表示退款金额，订单号为退款单号，status-订单状态为2表示已退款）。

问题：请计算2021年10月以来店铺901中商品毛利率大于24.9%的商品信息及店铺整体毛利率。

注：商品毛利率=(1-进价/平均单件售价)*100%；
店铺毛利率=(1-总进价成本/总销售收入)*100%。
结果先输出店铺毛利率，再按商品ID升序输出各商品毛利率，均保留1位小数。

输出示例：
示例数据的输出结果如下：
product_id  profit_rate
店铺汇总	    31.0%
8001	    29.4%
8003	    33.3%
解释：
店铺901有两件商品8001和8003；8001售出了3件，销售总额为25500，进价总额为18000，毛利率为1-18000/25500=29.4%，8003售出了1件，售价为18000，进价为12000，毛利率为33.3%；
店铺卖出的这4件商品总销售额为43500，总进价为30000，毛利率为1-30000/43500=31.0%
  */
select * from tb_order_overall;
select * from tb_product_info;
select * from tb_order_detail;

/**
  with rollup 用法：对group by的各个分组再次聚合计算。
  此处计算毛利率，也要计算店铺汇总的毛利率，不能用max计算进价成本。
  因为with rollup再次聚合时需要sum()才能求得总进价成本。
  故分子分母都应该用sum()。
  */
SELECT if(product_id is null,'店铺汇总',product_id) product_id, CONCAT(profit_rate, "%") as profit_rate
from (
         select product_id,
                ROUND(100 * (1 - SUM(in_price * cnt) / SUM(price * cnt)), 1) as profit_rate
         from tb_order_detail
                  left join tb_product_info using (product_id)
                  left join tb_order_overall using (order_id)
         WHERE shop_id = 901
           and date(event_time) >= '2021-10-01'
         group by product_id
         with rollup
         having profit_rate > 24.9 OR product_id IS NULL
         order by product_id
     ) as t
;