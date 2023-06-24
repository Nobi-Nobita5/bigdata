/**
  https://www.nowcoder.com/discuss/370999617754853376?sourceSSR=search;
  https://blog.csdn.net/weixin_43597208/article/details/123151294
  就是给你一行日期，有个起始日期和结束日期，让你生成多行数据。

  lateral view + explode用法介绍:
  lateral view用于将单个行输入转换为多个行输出
  https://blog.csdn.net/weixin_38753213/article/details/115364880
)
 */

create external table demo_2
(
    date_1  string,
    date_2 string
);

insert overwrite table demo_2 values ("2022-01-01","2022-01-03"),("2022-01-05","2022-01-09");
/**
  lateral view posexplode用法
 */
select
        id_start+pos as id
from(
        select
            1 as id_start,
            100 as id_end
    ) m  lateral view posexplode(split(space(id_end-id_start), '')) t as pos, val;
/**
  题解
 */
select
    date_1,
    date_2,
    pos,
    val,
    date_add(date_1,pos) as curr_date
from
    demo_2
        lateral view posexplode(split(space(datediff(date_2,date_1)),"")) tmp as pos,val;





