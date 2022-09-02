package yarn_monitor;

import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class HttpsClient {

    public String HttpsClient(String url) {

        String message = null;

        /**
         * 创建连接池
         */
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        // 设置最大连接数
        cm.setMaxTotal(200);
        // 设置每个主机的最大连接数
        cm.setDefaultMaxPerRoute(20);

        /**
         * 创建 httpclient 对象，不是每次创建新的HttpClient，而是从连接池中获取HttpClient对象
         */
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)  //设置PoolingHttpClientConnectionManager连接池
                .evictIdleConnections(60, TimeUnit.SECONDS)  //启用空闲连接驱逐策略，最大空闲时间为60秒
                .setRetryHandler(retry)
                .build();

        /**
         * 创建HttpGet对象，设置url访问地址
         * 举例：HttpGet httpGet = new HttpGet("http://10.60.79.100:8088/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default");
         */
        HttpGet httpGet = new HttpGet(url);

        /**
         * 设置请求参数
         */
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(35000)  // 连接主机服务超时时间，连接上服务器(握手成功)的时间，超出该时间抛出connect timeout
                .setConnectionRequestTimeout(35000)  // 请求超时时间,从连接池中获取连接的超时时间，超过该时间未拿到可用连接，会抛出：ConnectionPoolTimeoutException: Timeout waiting for connection from pool
                .setSocketTimeout(600000)  // 数据读取超时时间，单位（MS），服务器返回数据(response)的时间，超过该时间抛出read time out
                .build();
        httpGet.setConfig(requestConfig);


        CloseableHttpResponse response = null;
        try {
            /**
             * 使用HttpClient发起请求，获取response
             */
            response = httpClient.execute(httpGet);

            /**
             * 解析响应,判断响应状态码是否为200
             */
            if (response.getStatusLine().getStatusCode() == 200) {
                /**
                 * 如果为200表示请求成功，获取返回数据
                 */
                HttpEntity httpEntity = response.getEntity();
                message = EntityUtils.toString(httpEntity,"UTF-8");
            }
        } catch (ClientProtocolException e) {
            System.err.println("协议错误");
            e.printStackTrace();
        } catch (ParseException e) {
            System.err.println("解析错误");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("IO错误");
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    System.out.println("释放链接错误");
                    e.printStackTrace();
                }
                //不能关闭HttpClient，由连接池管理HttpClient
                //httpClient.close();
            }
            return message;
        }
    }

    /**
     *  测出超时重试机制为了防止超时不生效而设置
     *  如果直接放回false,不重试
     *  这里会根据情况进行判断是否重试
     */
    HttpRequestRetryHandler retry = new HttpRequestRetryHandler(){
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            if (executionCount >= 3) {// 如果已经重试了3次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return true;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// ssl握手异常
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        }
    };
}
