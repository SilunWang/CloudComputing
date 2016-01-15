package cc.cmu.edu.minisite;

/**
 * Created by Silun Wang on 11/4/15.
 */
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;
import org.json.JSONArray;

public class FollowerServlet extends HttpServlet {

    final static String driver = "com.mysql.jdbc.Driver";
    final static String url = "jdbc:mysql://127.0.0.1:3306/project34?useUnicode=true&characterEncoding=utf-8";
    final static String user = "root";
    final static String password = "15319project";
    static Connection conn;

    // HBase configs
    static Configuration conf = null;
    static HTable htable;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ec2-54-173-193-194.compute-1.amazonaws.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "ec2-54-173-193-194.compute-1.amazonaws.com:60000");
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FollowerServlet() {
        /**
         * Connect to MySQL
         */
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if(!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");

        } catch(ClassNotFoundException e) {
            System.out.println("Sorry,can't find the Driver!");
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        /**
         * Connect to Hbase
         */
        try {
            htable = new HTable(conf, Bytes.toBytes("graph"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String id = request.getParameter("id");

        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();

        Get g = new Get(Bytes.toBytes(id));
        Result r = htable.get(g);
        byte[] value = r.getValue(Bytes.toBytes("node"), Bytes.toBytes("data"));

        String valueStr = Bytes.toString(value);
        String[] arrStr = valueStr.split("-");
        ArrayList<JSONObject> list = new ArrayList();

        for (String x : arrStr) {
            System.out.println(x);
            try {
                Statement statement = conn.createStatement();
                String usersSQL = "SELECT * FROM userinfo WHERE id = " + x + " ;";
                ResultSet rs = statement.executeQuery(usersSQL);
                if (rs.next()) {
                    JSONObject follower = new JSONObject();
                    follower.put("name", rs.getString("name"));
                    follower.put("profile", rs.getString("profile"));
                    list.add(follower);
                    //System.out.println(follower.toString());
                }
                rs.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        Collections.sort(list, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                if (o1.get("name").equals(o2.get("name")))
                    return o1.get("profile").toString().compareTo(o2.get("profile").toString());
                else
                    return o1.get("name").toString().compareTo(o2.get("name").toString());
            }
        });

        for (JSONObject obj : list) {
            followers.put(obj);
        }

        if (arrStr.length == 0) {
            JSONObject follower = new JSONObject();
            follower.put("name", "Unauthorized");
            follower.put("profile", "#");
            followers.put(follower);
        }

        result.put("followers", followers);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
}


