/*
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
  (8001, 901, '日用', 60, 1000, '2020-01-01 10:00:00'),
  (8002, 901, '零食', 140, 500, '2020-01-01 10:00:00'),
  (8003, 901, '零食', 160, 500, '2020-01-01 10:00:00'),
  (8004, 902, '零食', 130, 500, '2020-01-01 10:00:00');

INSERT INTO tb_order_overall(order_id, uid, event_time, total_amount, total_cnt, `status`) VALUES
  (301002, 102, '2021-10-01 11:00:00', 235, 2, 1),
  (301003, 101, '2021-10-02 10:00:00', 300, 2, 1),
  (301005, 104, '2021-10-03 10:00:00', 160, 1, 1);

INSERT INTO tb_order_detail(order_id, product_id, price, cnt) VALUES
  (301002, 8001, 85, 1),
  (301002, 8003, 180, 1),
  (301003, 8004, 140, 1),
  (301003, 8003, 180, 1),
  (301005, 8003, 180, 1);
--------------------------------------------------------------
问题：请计算2021年10月商城里所有新用户的首单平均交易金额（客单价）和平均获客成本（保留一位小数）。

注：订单的优惠金额 = 订单明细里的{该订单各商品单价×数量之和} - 订单总表里的{订单总金额} 。

输出示例：
示例数据的输出结果如下
avg_amount	avg_cost
231.7	23.3
解释：
2021年10月有3个新用户，102的首单为301002，订单金额为235，商品总金额为85+180=265，优惠金额为30；
101的首单为301003，订单金额为300，商品总金额为140+180=320，优惠金额为20；
104的首单为301005，订单金额为160，商品总金额为180，优惠金额为20；
平均首单客单价为(235+300+160)/3=231.7，平均获客成本为(30+20+20)/3=23.3
*/

select * from tb_order_detail;
select * from tb_order_overall;
select * from tb_product_info;

select round(sum(total_amount)/count(order_id),1) avg_amount, /*总消费价格 / 订单数量*/
       round(avg(cost),1) avg_cost /*平均优惠金额*/
from (select a.order_id,
             total_amount,
             (sum(price*cnt) - total_amount) as cost
      from tb_order_detail a
               left join tb_order_overall b
                         on a.order_id = b.order_id
      where date_format(event_time,'%Y-%m') = '2021-10'
        and (uid,event_time) in (select uid ,min(event_time)     -- 用户和其第一次购买的时间
                                 from tb_order_overall
                                 GROUP BY uid )
      GROUP BY 1,2) a
