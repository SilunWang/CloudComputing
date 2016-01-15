package cc.cmu.edu.minisite;
/**
 * Author: Silun Wang
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import org.jruby.RubyProcess;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class HomepageServlet extends HttpServlet {

    static AmazonDynamoDBClient client;
    static DynamoDBMapper mapper;

    public HomepageServlet() {
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

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        JSONArray arr = new JSONArray();
        Post post = new Post();
        post.setUserid(Integer.parseInt(id));

        DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>()
                .withHashKeyValues(post);

        List<Post> posts = mapper.query(Post.class, queryExpression);
        ArrayList<Post> list = new ArrayList(posts);

        Collections.sort(list, new Comparator<Post>() {
            @Override
            public int compare(Post o1, Post o2) {
                return o1.getTimestamp().compareTo(o2.getTimestamp());
            }
        });

        for (Post p : list) {
            arr.put(new JSONObject(p.getPost()));
        }

        result.put("posts", arr);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request,
                          final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}
