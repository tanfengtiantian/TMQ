package io.kafka.http;

import com.alibaba.fastjson.JSONObject;
import io.kafka.cluster.Broker;
import io.kafka.plugin.BrokerContext;
import io.kafka.utils.nettyloc.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JettyProcessor extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(JettyProcessor.class);

    @Override
    public void init(ServletConfig config) throws ServletException {
        // servlet init
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.info(req.getRequestURI());
        PrintWriter writer = resp.getWriter();
        writer.print("** -------------------dashboard------------------ **\n");
        writer.print(JettyBroker.context.getInstance());
        writer.print(JettyBroker.context.getSystem());
        writer.print(JettyBroker.context.getJVM());
        writer.print(JettyBroker.context.getBroker());
        writer.print(JettyBroker.context.getVersion());
        writer.print("\n** -------------------logger------------------ **\n");
        List<String> list = JettyBroker.context.getlog(0L);
        for (int i = 0; i < list.size(); i++) {
            writer.print(list.get(i));
        }
        writer.print("\n** -------------------cluster------------------ **\n");
        writer.print(JettyBroker.context.getCluster());
        writer.print("\n** -------------------java-properties------------------ **\n");
        writer.print(JettyBroker.context.getJavaProperties());
        writer.print("\n** -------------------threads-dump------------------ **\n");
        writer.print(JettyBroker.context.getThreadsDump());
        writer.print("\n** -------------------Broker-config------------------ **\n");
        //writer.print(JettyBroker.context.getConfig());
        writer.print("\n** -------------------Topics------------------- **\n");
        writer.print(JettyBroker.context.getTopics());

        writer.print("\n** -------------------Delete-Topics------------------- **\n");
        writer.print("Delete-Topics 数量="+JettyBroker.context.getLogManager().deleteLogs("tf"));
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String cmd = req.getParameter("cmd");
        String data = req.getParameter("data");
        logger.info("【Metric 查询请求】cmd = {} data = {} time = {}", cmd, data, LocalDateTime.now());
        String response = dispatch(cmd, data);
        resp.setContentType("applicaion/json;charset=utf-8");
        PrintWriter writer = resp.getWriter();
        writer.print(response);
    }

    /**
     * 调度器
     */
    private String dispatch (String cmd, String data) {
        String response;
        switch (cmd) {
            case "metric/dashboard":
                response = queryDashboard();
                break;
            case "metric/logging":
                response = queryLogging();
                break;
            case "metric/cluster":
                response = queryCluster();
                break;
            case "metric/java-properties":
                response = queryJavaProperties();
                break;
            case "metric/thread-dump":
                response = queryThreadDump();
                break;
            case "metric/broker-config":
                response = queryBrokerConfig();
                break;
            case "metric/topics":
                response = queryTopics();
                break;
            case "metric/deleteTopic":
                response = doDeleteTopic(data);
                break;
            default:
                response = "404 Not found";
        }
        return response;
    }

    /**
     * 查询控制台信息
     */
    private String queryDashboard () {
        BrokerContext context = JettyBroker.context;
        Map<String, Object> data = new HashMap<>(10);
        data.put("instance", context.getInstance());
        data.put("system", context.getSystem());
        data.put("jvm", context.getJVM());
        data.put("broker", context.getBroker());
        data.put("version", context.getVersion());
        return JSONObject.toJSONString(data);
    }

    /**
     * 查询日志
     */
    private String queryLogging () {
        BrokerContext context = JettyBroker.context;
        List<String> list = context.getlog(0L);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询集群信息
     */
    private String queryCluster () {
        List<Broker> cluster = JettyBroker.context.getCluster();
        return JSONObject.toJSONString(cluster);
    }

    /**
     * 查询Java Properties
     */
    private String queryJavaProperties () {
        return JSONObject.toJSONString(JettyBroker.context.getJavaProperties());
    }

    /**
     * 查询Thread Dump
     */
    private String queryThreadDump () {
        return JSONObject.toJSONString(JettyBroker.context.getThreadsDump());
    }

    /**
     * 查询Broker Config
     */
    private String queryBrokerConfig () {
        return JSONObject.toJSONString(JettyBroker.context.getConfig());
    }

    /**
     * 查询Topics
     */
    private String queryTopics () {
        return JSONObject.toJSONString(JettyBroker.context.getTopics());
    }

    /**
     * 删除Topic
     */
    private String doDeleteTopic (String data) {
        JSONObject jsonObject = JSONObject.parseObject(data);
        String topic = jsonObject.getString("topic");
        if (topic != null && !topic.isEmpty()) {
            JettyBroker.context.getLogManager().deleteLogs(topic);
            logger.info("【Broker删除Topic】删除成功 topic = {}", topic);
            return "SUCCESS";
        } else {
            return "ERROR";
        }
    }
}