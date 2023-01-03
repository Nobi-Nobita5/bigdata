import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/29/15:35
 * @Version: 1.0
 */
public class HbaseDQL {
    //添加静态属性connection单例链接
    private static Connection connection = HBaseConnect.connection;

    /***
     * 用PageFilter（可以返回指定limit行数的结果集）实现对表的分页查询
     * @param tableName
     * @throws IOException
     */
    public void PageFilterDemo(String tableName) throws IOException {
        byte[] POSTFIX = new byte[] { 0x00 };//0x表示十六进制
        Filter filter = new PageFilter(15);//设置返回结果集的条数
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int totalRows = 0;
        byte[] lastRow = null;
        while (true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                // 如果不是首行 则 lastRow + 0
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                System.out.println("start row: " +
                        Bytes.toStringBinary(startRow));
                scan.withStartRow(startRow);//scan设置开始扫描的行数，如果每次都从第一行开始，那么数据会重复
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
                System.out.println(localRows++ + ": " + result);//键值对的方式打印每行数据
                totalRows++;
                lastRow = result.getRow();
            }
            scanner.close();
            //最后一页，查询结束
            if (localRows == 0) break;//如果本次scan没有查出数据，说明当前的lastRow在表中已不存在，遍历完成
        }
        System.out.println("total rows: " + totalRows);
    }
}
