package yarn_monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 此模块主要是用来进行 Yarn 的主节点判断
 * 判断哪个节点是 Active 哪个节点是 Standby
 * 然后返回 Active 节点 IP
 */
public class YarnCluseterInfo {

    public static String Yarn_Cluster_Info(String IP1, String IP2){

        String IP = null;
        String url1="http://"+ IP1 +":8088/ws/v1/cluster/info";

        // 创建http解析方法对象
        HttpsClient HC = new HttpsClient();
        // 获取链接对应的块信息
        String message1 = HC.HttpsClient(url1);

        //将一行 jSON 数据转换为 JSON 对象
        JSONObject jsonObject1 = JSON.parseObject(message1);

        //因为 clusterInfo 在JSON数据中是集合{}，所有用 JSON 对象的 getJSONObject 来获取内容
        JSONObject clusterInfo1 = jsonObject1.getJSONObject("clusterInfo");

        if (clusterInfo1.get("haState").equals("ACTIVE")){
            IP = IP1;
        }else{
            IP = IP2;
        }

        return IP;
    }

}
