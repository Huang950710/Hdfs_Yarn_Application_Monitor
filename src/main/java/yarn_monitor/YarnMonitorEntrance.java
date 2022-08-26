package yarn_monitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class YarnMonitorEntrance {

    /**
     * 主函数入口
     * @param args
     */
    public static void main(String[] args) {

        /**
         * 参数判断
         * 1.参数个数不等于3的，直接退出程序
         * 2.发送模块不存在的，直接退出程序
         */
        if (args.length != 3) {
            System.out.println("参数个数错误");
            System.exit(1);
        }

        /**
         * 主节点判断
         */
        String IP = YarnCluseterInfo.Yarn_Cluster_Info(args[1], args[2]);
        String COMPANY = args[0];

        /**
         * 获取监控数据方法，并写入数据库
         */
        YarnClusterApplication.YARN_CLUSTER_APPLICATION_JSON(COMPANY,IP);
    }
}
