package yarn_monitor;

public class Influxdb {

    private String openurl ;//连接地址
    private String username ;//用户名
    private String password ;//密码
    private String database ;//数据库

    public Influxdb() {
    }

    public Influxdb(String openurl, String username, String password, String database) {
        this.openurl = openurl;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getOpenurl() {
        return openurl;
    }

    public void setOpenurl(String openurl) {
        this.openurl = openurl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
