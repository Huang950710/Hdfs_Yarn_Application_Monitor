package yarn_monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class YarnClusterApplication extends Thread {

    /**
     * tags ：influxdb数据库 tag 数据
     * fields1，fields2 存放 influxdb 的 fields 数据
     */
    private final Map<String, String> tags = new HashMap<>();
    private final Map<String, Object> fields = new HashMap<>();
    private String COMPANY;
    private String IP;
    private final Infludb_client setup = Infludb_client.setup();

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
        while (true) {
            synchronized (YarnClusterApplication.class) {
                this.YARN_CLUSTER_APPLICATION_JSON(COMPANY, IP);
            }
        }
    }

    public YarnClusterApplication(String COMPANY, String IP) {
        this.COMPANY = COMPANY;
        this.IP = IP;
    }

    public void YARN_CLUSTER_APPLICATION_JSON(String COMPANY, String IP) {

        /**
         * ****************************************************************************************************************************************************************
         * Influxdb 数据赋值
         */
        //调用setup方法，初始化influxdb链接
//        Infludb_client setup = Infludb_client.setup();

/*       // 删除MEASUREMENT（删除表）
         setup.query("DROP MEASUREMENT HDFS_YARN_APPLICATION_MONITOR");
*/

        /**
         * ****************************************************************************************************************************************************************
         * 时间计算
         */
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat time1 = new SimpleDateFormat("yyyyMMddHHmmss");
        Date aa = new Date();

        /**
         * tag值标记的数据是那个业务线的数据
         */
        tags.put("company", COMPANY);

        String url1 = "http://" + IP + ":8088/ws/v1/cluster/apps";

        // 创建http解析方法对象
        HttpsClient HC = new HttpsClient();
        // 获取链接对应的块信息
        String message1 = null;
        message1 = HC.HttpsClient(url1);

        //将一行 jSON 数据转换为 JSON 对象
        JSONObject jsonObject1 = JSON.parseObject(message1);

        Object o = JSON.parseObject(jsonObject1.get("apps").toString()).getJSONArray("app");
        JSONArray objects = JSONArray.parseArray(o.toString());

        for (Object object : objects) {
            JSONObject jsonObject = JSON.parseObject(object.toString());
            /**
             * 获取正在运行的任务ID
             */
            if (jsonObject.get("state").toString().equals("RUNNING") || jsonObject.get("state").toString().equals("ACCEPTED")) {
                /**
                 * 数据插入时间
                 */
                fields.put("DATETIME", time1.format(aa));
                /**
                 * 任务ID
                 */
                fields.put("ID", jsonObject.get("id"));
                /**
                 * 应用程序名称
                 */
                fields.put("NAME", jsonObject.get("name").toString());
                /**
                 * 启动应用程序用户
                 */
                fields.put("USER", jsonObject.get("user"));
                /**
                 * 提交任务队列
                 */
                fields.put("QUEUE", jsonObject.get("queue"));
                /**
                 * 任务状态
                 */
                fields.put("STATE", jsonObject.get("state"));
                /**
                 * 启动时间
                 */
                fields.put("STARTEDTIME", time.format(Long.parseLong(jsonObject.get("startedTime").toString())));
                /**
                 * 任务耗时
                 */
                fields.put("ELAPSEDTIME", Integer.parseInt(jsonObject.get("elapsedTime").toString()) / 1000);
                /**
                 * 以百分比表示应用程序的进度
                 */
                fields.put("PROGRESS", jsonObject.get("progress"));
                /**
                 * 可用于跟踪应用程序 ApplicationMaster 的 Web URL：
                 */
                fields.put("TRACKINGURL", jsonObject.get("trackingUrl"));
                /**
                 * 应用程序类型
                 */
                fields.put("APPLICATIONTYPE", jsonObject.get("applicationType"));
                /**
                 * 任务优先权
                 */
                fields.put("PRIORITY", jsonObject.get("priority"));
                /**
                 * 分配给应用程序运行容器的内存总量(MB)
                 */
                fields.put("ALLOCATEDMB", jsonObject.get("allocatedMB"));
                /**
                 * 分配给应用程序运行容器的虚拟核的总和
                 */
                fields.put("ALLOCATEDVCORES", jsonObject.get("allocatedVCores"));
                /**
                 * 当前为应用程序运行的容器数
                 */
                fields.put("RUNNINGCONTAINERS", jsonObject.get("runningContainers"));
                /**
                 * 应用程序正在使用的队列资源的百分比
                 */
                fields.put("QUEUEUSAGEPERCENTAGE", jsonObject.get("queueUsagePercentage"));
                /**
                 * 应用程序正在使用的集群资源的百分比
                 */
                fields.put("CLUSTERUSAGEPERCENTAGE", jsonObject.get("clusterUsagePercentage"));

                setup.insert(tags, fields, "HDFS_CLUSTER_YARN_APPLICATION");
//                setup.close();
                fields.clear();
            }
        }
    }
}
