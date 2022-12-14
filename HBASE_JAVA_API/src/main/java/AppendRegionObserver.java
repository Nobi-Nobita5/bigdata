import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Xionghx
 * @Date: 2022/07/30/13:34
 * @Version: 1.0
 */
public class AppendRegionObserver implements RegionObserver {
    private byte[] columnFamily = Bytes.toBytes("article");
    private byte[] qualifier = Bytes.toBytes("content");
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (put.has(columnFamily, qualifier)) {
            // 遍历查询结果，获取指定列的原值
            Result rs = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            String oldValue = "";
            for (Cell cell : rs.rawCells())
                if (CellUtil.matchingColumn(cell, columnFamily, qualifier)) {
                    oldValue = Bytes.toString(CellUtil.cloneValue(cell));
                }

            // 获取指定列新插入的值
            List<Cell> cells = put.get(columnFamily, qualifier);
            String newValue = "";
            for (Cell cell : cells) {
                if (CellUtil.matchingColumn(cell, columnFamily, qualifier)) {
                    newValue = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }

            // Append 操作
            put.addColumn(columnFamily, qualifier, Bytes.toBytes(oldValue + newValue));
        }
    }
}
