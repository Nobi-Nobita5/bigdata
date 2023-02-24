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
  (8003, 901, '零食', 160, 500, '2020-01-01 10:00:00');

INSERT INTO tb_order_overall(order_id, uid, event_time, total_amount, total_cnt, `status`) VALUES
  (301004, 102, '2021-09-30 10:00:00', 170, 1, 1),
  (301005, 104, '2021-10-01 10:00:00', 160, 1, 1),
  (301003, 101, '2021-10-02 10:00:00', 300, 2, 1),
  (301002, 102, '2021-10-03 11:00:00', 235, 2, 1);

INSERT INTO tb_order_detail(order_id, product_id, price, cnt) VALUES
  (301004, 8002, 180, 1),
  (301005, 8002, 170, 1),
  (301002, 8001, 85, 1),
  (301002, 8003, 180, 1),
  (301003, 8002, 150, 1),
  (301003, 8003, 180, 1);
--------------------------------------------------
问题：请计算店铺901在2021年国庆头3天的7日动销率和滞销率，结果保留3位小数，按日期升序排序。

注：
动销率定义为店铺中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数)。
滞销率定义为店铺中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品/已上架总商品数)。
只要当天任一店铺有任何商品的销量就输出该天的结果，即使店铺901当天的动销率为0。

输出示例：
示例数据的输出结果如下：
dt	        sale_rate	unsale_rate
2021-10-01	0.333	0.667
2021-10-02  0.667	0.333
2021-10-03  1.000	0.000
解释：
10月1日的近7日（9月25日---10月1日）店铺901有销量的商品有8002，截止当天在售商品数为3，动销率为0.333，滞销率为0.667；
10月2日的近7日（9月26日---10月2日）店铺901有销量的商品有8002、8003，截止当天在售商品数为3，动销率为0.667，滞销率为0.333；
10月3日的近7日（9月27日---10月3日）店铺901有销量的商品有8002、8003、8001，截止当天店铺901在售商品数为3，动销率为1.000，
滞销率为0.000；*/


SELECT dt, ROUND(cnt / total_cnt, 3) AS sale_rate, ROUND(1 - cnt / total_cnt, 3) AS unsale_rate
FROM
    (
        SELECT DISTINCT
            DATE(event_time) AS dt,
            (
                SELECT COUNT(DISTINCT (IF(shop_id != 901, null, product_id)))
                FROM tb_order_overall
                         JOIN tb_order_detail USING (order_id)
                         JOIN tb_product_info USING (product_id)
                WHERE TIMESTAMPDIFF(DAY, event_time, to1.event_time) BETWEEN 0 AND 6
            ) AS cnt,
            (
                SELECT COUNT(DISTINCT product_id)
                FROM tb_product_info
                WHERE shop_id = 901
            ) AS total_cnt
        FROM tb_order_overall to1
        WHERE DATE(event_time) BETWEEN '2021-10-01' AND '2021-10-03'
    ) AS t0
ORDER BY dt;
