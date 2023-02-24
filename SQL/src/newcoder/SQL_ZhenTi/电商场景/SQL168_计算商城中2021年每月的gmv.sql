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
  (301001, 101, '2021-10-01 10:00:00', 15900, 2, 1),
  (301002, 101, '2021-10-01 11:00:00', 15900, 2, 1),
  (301003, 102, '2021-10-02 10:00:00', 34500, 8, 0),
  (301004, 103, '2021-10-12 10:00:00', 43500, 9, 1),
  (301005, 105, '2021-11-01 10:00:00', 31900, 7, 1),
  (301006, 102, '2021-11-02 10:00:00', 24500, 6, 1),
  (391007, 102, '2021-11-03 10:00:00', -24500, 6, 2),
  (301008, 104, '2021-11-04 10:00:00', 55500, 12, 0);
------------------------------
  场景逻辑说明：
用户将购物车中多件商品一起下单时，订单总表会生成一个订单（但此时未付款，status-订单状态为0，表示待付款）；
当用户支付完成时，在订单总表修改对应订单记录的status-订单状态为1，表示已付款；
若用户退货退款，在订单总表生成一条交易总金额为负值的记录（表示退款金额，订单号为退款单号，status-订单状态为2表示已退款）。

问题：请计算商城中2021年每月的GMV，输出GMV大于10w的每月GMV，值保留到整数。

注：GMV为已付款订单和未付款订单两者之和。结果按GMV升序排序。
----------------------
输出：
month    GMV
2021-10|109800
2021-11|111900
 */

select * from tb_order_overall;

select date_format(t.event_time,'%Y-%m') month, round(sum(total_amount),0) GMV
from tb_order_overall t
where date_format(t.event_time,'%Y') = '2021' and t.status != 2
group by month
having GMV > 100000
order by GMV asc