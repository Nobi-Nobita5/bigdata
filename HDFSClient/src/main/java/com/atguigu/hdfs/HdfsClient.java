package com.atguigu.hdfs;

import org.apache.hadoop.fs.*;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Author: Xionghx
 * @Date: 2022/06/28/11:19
 * @Version: 1.0
 */
public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, URISyntaxException,InterruptedException {
        Configuration configuration = new Configuration();
        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),configuration,"xionghx");
        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试dfs.replication参数优先级
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException,
            InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        //configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
                configuration, "xionghx");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("D:/Project/test/HDFSClient/sunwukong.txt"), new
                Path("/xiyou/huaguoshan/sunwukong.txt"));
        // 3 关闭资源
        fs.close();
        }
    //HDFS 文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),configuration,"xionghx");
        //2.执行下载操作
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("D:/Project/test/HDFSClient/sunwukong2.txt"),true);
        //3.关闭资源
        fs.close();
    }

    //HDFS文件更名和移动
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),configuration,"xionghx");
        //2.修改文件名称
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("/xiyou/huaguoshan/meihouwang.txt"));
        //3.关闭资源
        fs.close();
    }

    //HDFS 删除文件和目录
    @Test
    public void testDelete() throws IOException, InterruptedException,
            URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
                configuration, "xionghx");
        // 2 执行删除
        fs.delete(new Path("/xiyou"), true);
        // 3 关闭资源
        fs.close();
    }
    //HDFS 文件详情查看
    //查看文件名称、权限、长度、块信息
    @Test
    public void testListFiles() throws IOException, InterruptedException,
            URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
                configuration, "xionghx");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),
                true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        // 3 关闭资源
        fs.close();
    }


    //HDFS 文件和文件夹判断
    @Test
    public void testListStatus() throws IOException, InterruptedException,
            URISyntaxException{
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
                configuration, "xionghx");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3 关闭资源
        fs.close();
    }


}
