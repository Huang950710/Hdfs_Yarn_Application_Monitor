package yarn_monitor;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.Map;

public class Infludb_client {

    private static String openurl = "http://10.60.79.103:8086";
    private static String username = "admin";
    private static String password = "admin";
    private static String database = "yarn";

    private InfluxDB influxDB;

    public Infludb_client(String username, String password, String openurl, String database) {
        this.username = username;
        this.password = password;
        this.openurl = openurl;
        this.database = database;
    }

    //方法初始化
    public static Infludb_client setup() {
        //1.创建一个Infludb_client的对象
        Infludb_client infludb_client = new Infludb_client(username, password, openurl, database);
        //2.调用创建 influxdb 数据库的链接
        infludb_client.influxDbBuild();
        //3.返回一个该类的对象
        return infludb_client;
    }

    //创建 influxdb 链接
    public InfluxDB influxDbBuild() {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(openurl, username, password);
        }
        return influxDB;
    }

    /**
     * 创建数据库
     */
    public void createdatabase(){
        /**
         * 判断数据库是否存在，不存在创建数据库
         */
        if (!influxDB.databaseExists(database)) {
            /**
             * 创建数据库
             */
            influxDB.createDatabase(database);
            /**
             * 创建数据库策略
             */
            this.createRetentionPolicy();
        }
    }

    /**
     * 创建数据库策略
     */
    public void createRetentionPolicy() {
        String command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT", "defalut", database, "15d", 1);
        this.query(command);
    }

    /**
     * * 查询
     * * @param command 查询语句
     * * @return
     */
    public QueryResult query(String command) {
        return influxDB.query(new Query(command, database));
    }

    /**
     * * 插入
     * * @param tags 标签
     * * @param fields 字段
     */
    public void insert(Map<String, String> tags, Map<String, Object> fields, String measurement) {
        Point.Builder builder = Point.measurement(measurement);
        builder.tag(tags);
        builder.fields(fields);
        influxDB.write(database, "", builder.build());
    }

    public void close() {
        influxDB.close();
    }
}
