package cc.cmu.edu.minisite;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess;
import org.json.JSONArray;
import org.json.JSONObject;

public class TimelineServlet extends HttpServlet {

    static AmazonDynamoDBClient client;
    static DynamoDBMapper mapper;
    final static String driver = "com.mysql.jdbc.Driver";
    final static String url = "jdbc:mysql://127.0.0.1:3306/project34?useUnicode=true&characterEncoding=utf-8";
    final static String user = "root";
    final static String password = "15319project";
    static Connection conn;

    // HBase configs
    static Configuration conf = null;
    static HTable htable;
    static HTable htable2;
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

    public TimelineServlet() throws Exception {
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
            htable2 = new HTable(conf, Bytes.toBytes("graph2"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        /**
         * Connect to DynamoDB
         */
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BasicAWSCredentials credentials = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));
        client = new AmazonDynamoDBClient(credentials);
        mapper = new DynamoDBMapper(client);
    }

    @DynamoDBTable(tableName="project34")
    public static class Post {
        private int userid;
        private String timestamp;
        private String post;

        @DynamoDBHashKey(attributeName="UserID")
        public int getUserid() { return userid; }
        public void setUserid(int id) { this.userid = id; }

        @DynamoDBAttribute(attributeName="Timestamp")
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String ts) { this.timestamp = ts; }

        @DynamoDBAttribute(attributeName="Post")
        public String getPost() { return post; }
        public void setPost(String posts) { this.post = posts;}
    }


    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        // user id
        String id = request.getParameter("id");

        /**
         * my own profile
         */
        try {
            Statement statement = conn.createStatement();
            String usersSQL = "SELECT * FROM userinfo WHERE id = " + id + " ;";
            ResultSet rs = statement.executeQuery(usersSQL);
            System.out.println(usersSQL);
            if (rs.next()) {
                System.out.println("I'm in");
                result.put("name", rs.getString("name"));
                result.put("profile", rs.getString("profile"));
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        /**
         * My followers
         */
        JSONArray followers = new JSONArray();

        Get g = new Get(Bytes.toBytes(id));
        Result r = htable.get(g);
        byte[] value = r.getValue(Bytes.toBytes("node"), Bytes.toBytes("data"));
        String valueStr = Bytes.toString(value);
        String[] arrStr = valueStr.split("-");
        ArrayList<JSONObject> list = new ArrayList();
        for (String x : arrStr) {
            try {
                Statement statement = conn.createStatement();
                String usersSQL = "SELECT * FROM userinfo WHERE id = " + x + " ;";
                ResultSet rs = statement.executeQuery(usersSQL);
                if (rs.next()) {
                    JSONObject follower = new JSONObject();
                    follower.put("name", rs.getString("name"));
                    follower.put("profile", rs.getString("profile"));
                    list.add(follower);
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
        result.put("followers", followers);

        /**
         * Latest posts
         */

        Get g2 = new Get(Bytes.toBytes(id));
        Result r2 = htable2.get(g2);
        byte[] value2 = r2.getValue(Bytes.toBytes("node"), Bytes.toBytes("data"));
        String valueStr2 = Bytes.toString(value2);
        String[] arrStr2 = valueStr2.split("-");

        JSONArray arr = new JSONArray();
        Post post = new Post();
        ArrayList<Post> ls = new ArrayList();
        // for every follower
        for (String x : arrStr2) {
            post.setUserid(Integer.parseInt(x));
            DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>()
                    .withHashKeyValues(post);
            List<Post> posts = mapper.query(Post.class, queryExpression);
            ls.addAll(posts);
        }
        // sort array
        Collections.sort(ls, new Comparator<Post>() {
            @Override
            public int compare(Post o1, Post o2) {
                if (o1.getTimestamp().equals(o2.getTimestamp())) {
                    System.out.println(o1.getPost());
                    return 1;
                } else
                    return o1.getTimestamp().compareTo(o2.getTimestamp());
            }
        });
        // at most 30
        for (int i = 0; i < 30 && i < ls.size(); i++) {
            arr.put(new JSONObject(ls.get(ls.size() -  30 + i).getPost()));
        }

        result.put("posts", arr);

        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
}
