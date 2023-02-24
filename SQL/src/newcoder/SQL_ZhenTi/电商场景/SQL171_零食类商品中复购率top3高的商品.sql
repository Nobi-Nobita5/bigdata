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
  (8001, 901, '零食', 60, 1000, '2020-01-01 10:00:00'),
  (8002, 901, '零食', 140, 500, '2020-01-01 10:00:00'),
  (8003, 901, '零食', 160, 500, '2020-01-01 10:00:00');

INSERT INTO tb_order_overall(order_id, uid, event_time, total_amount, total_cnt, `status`) VALUES
  (301001, 101, '2021-09-30 10:00:00', 140, 1, 1),
  (301002, 102, '2021-10-01 11:00:00', 235, 2, 1),
  (301011, 102, '2021-10-31 11:00:00', 250, 2, 1),
  (301003, 101, '2021-11-02 10:00:00', 300, 2, 1),
  (301013, 105, '2021-11-02 10:00:00', 300, 2, 1),
  (301005, 104, '2021-11-03 10:00:00', 170, 1, 1);

INSERT INTO tb_order_detail(order_id, product_id, price, cnt) VALUES
  (301001, 8002, 150, 1),
  (301011, 8003, 200, 1),
  (301011, 8001, 80, 1),
  (301002, 8001, 85, 1),
  (301002, 8003, 180, 1),
  (301003, 8002, 140, 1),
  (301003, 8003, 180, 1),
  (301013, 8002, 140, 2),
  (301005, 8003, 180, 1);
-----------------------------------
场景逻辑说明：
用户将购物车中多件商品一起下单时，订单总表会生成一个订单（但此时未付款， status-订单状态-订单状态为0表示待付款），
在订单明细表生成该订单中每个商品的信息；

当用户支付完成时，在订单总表修改对应订单记录的status-订单状态-订单状态为1表示已付款；

若用户退货退款，在订单总表生成一条交易总金额为负值的记录（表示退款金额，订单号为退款单号，订单状态为2表示已退款）。


问题：请统计零食类商品中复购率top3高的商品。

注：复购率指用户在一段时间内对某商品的重复购买比例，复购率越大，则反映出消费者对品牌的忠诚度就越高，也叫回头率
此处我们定义：某商品复购率 = 近90天内购买它至少两次的人数 ÷ 购买它的总人数
近90天指包含最大日期（记为当天）在内的近90天。结果中复购率保留3位小数，并按复购率倒序、商品ID升序排序

输出示例：
示例数据的输出结果如下：
product_id	repurchase_rate
8001	    1.000
8002	    0.500
8003	    0.333
解释：
商品8001、8002、8003都是零食类商品，
商品8001只被用户102购买了两次，复购率1.000；
商品8002被101购买了两次，被105购买了1次，复购率0.500；
商品8003被102购买两次，被101和105各购买1次，复购率为0.333。*/

select * from tb_order_detail;
select * from tb_order_overall;
select * from tb_product_info;

SELECT product_id,
       ROUND(SUM(repurchase) / COUNT(repurchase), 3) as repurchase_rate /*用count()代表购买总人数，sum()代表复购人数*/
FROM (
         SELECT uid, product_id, IF(COUNT(event_time)>1, 1, 0) as repurchase /*每个商品每个用户购买的次数大于1，则标记为复购*/
         FROM tb_order_detail
                  JOIN tb_order_overall USING(order_id)
                  JOIN tb_product_info USING(product_id)
         WHERE tag='零食' AND event_time >= (
             SELECT DATE_SUB(MAX(event_time), INTERVAL 89 DAY)
             FROM tb_order_overall
         )
         GROUP BY uid, product_id
     ) as t_uid_product_info
GROUP BY product_id
ORDER BY repurchase_rate DESC, product_id
LIMIT 3;


with tmp as (
    select t1.product_id, t2.uid, count(*) count3 /*每个商品每个用户购买的次数*/
    from tb_order_detail t1
             left join tb_order_overall t2 on t1.order_id = t2.order_id
             left join tb_product_info t3 on t1.product_id = t3.product_id
             left join (
        select product_id, max(event_time) max_event_time
        from tb_order_detail
                 left join tb_order_overall using (order_id)
        group by product_id
    ) t4 on t1.product_id = t4.product_id
    where tag = '零食'
      and timestampdiff(day, event_time, max_event_time) < 90
    group by t1.product_id, t2.uid
)
# select product_id,uid,count3 count2 from tmp;
select t1.product_id,round(count1 / count2,3) repurchase_rate
       from (
           select product_id,count(uid) count2 from tmp group by product_id/*购买该商品的总人数*/
           ) t1
left join (
    select product_id,count(uid) count1 from tmp where count3 >= 2 group by product_id /*购买该商品次数大于等于2的人数*/
    ) t2 on t1.product_id = t2.product_id
order by repurchase_rate desc ,product_id asc
LIMIT 3;