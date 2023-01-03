import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
/**
 * @Author: Xionghx
 * @Date: 2022/07/28/17:29
 * @Version: 1.0
 * 单例模式创建该类
 *
 * 根据官方 API 介绍，HBase 的客户端连接由 ConnectionFactory 类来创建，用户使用完成
 * 之后需要手动关闭连接。同时连接是一个重量级的，推荐一个进程使用一个连接，对 HBase
 * 的命令通过连接中的两个属性 Admin 和 Table 来实现。
 */
public class HBaseConnect {
    //准备一个类属性，用于指向一个实例化对象，但是暂时指向null
    public static Connection connection = null;

    /**构造方法私有化
     * 但此处Connection的代码不是我们写的，我们使用以下方式实现：
     * 在JVM类加载HBaseConnect类时就用静态代码块创建好该连接，后续多个线程
     * 都是调用该连接即可。
     */
    static {
        try {
            // 使用配置文件的方法
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }

    /**
     * 连接关闭方法,用于进程关闭时调用
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if (connection != null){
            connection.close();
        }
    }
}
