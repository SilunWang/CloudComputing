package io.vertx.example.util;

import io.vertx.core.AbstractVerticle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * @author Silun Wang
 */
public final class Main extends AbstractVerticle {

    private static ExecutorService executor = Executors.newFixedThreadPool(100);

    private final String teamInfo = "Up In The Cloud,046409469654\n";
    private static BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
    private static BigInteger int25 = new BigInteger("25");

    // MYSQL configs
    private final static String ips[] = {"127.0.0.1"};
    private final static String db_name = "db15619";
    private final static String username = "newuser";
    private final static String password = "password";
    private static Connection[] conns = new Connection[ips.length];
    private final static String driver = "com.mysql.jdbc.Driver";
    private final static String[] urls = new String[ips.length];
    
    private static int[] q5uid = new int[53767998];
    private static int[] q5cnt = new int[53767998];

    boolean isMySQL = true;
    HTable q2htable;
    HTable q3htable;
    HTable q4htable;
    // hbase column name
    public static final byte[] CF = "cf".getBytes();
    public static final byte[] TWEETID_ATTR = "tweet_id".getBytes();
    public static final byte[] SCORE_ATTR = "score".getBytes();
    public static final byte[] TEXT_ATTR = "text".getBytes();
    public static final byte[] IMPACT_ATTR = "impact".getBytes();
    public static final byte[] TID_TEXT_ATTR = "tid_text".getBytes();
    public static final byte[] VALUE_ATTR = "result".getBytes();

    // mysql connection index
    private static int mySQLIdx = 0;
    // max mysql connection number
    private final int max_conn = ips.length;

    // HBase configs
    static Configuration conf = null;
    static {
        for (int i = 0; i < ips.length; i++) {
            urls[i] = "jdbc:mysql://" + ips[i] + "/" + db_name + "?useUnicode=true&characterEncoding=utf-8";
        }
        try{
            BufferedReader br = new BufferedReader(new FileReader("/home/ubuntu/q5sqlsort"));
            try {
                String line = br.readLine();
                int idx = 0;
                while (line != null) {
                    String[] arr = line.split("\t");
                    q5uid[idx] = (int)(Long.parseLong(arr[0])-Integer.MAX_VALUE);
                    q5cnt[idx] = Integer.parseInt(arr[1]);
                    if (idx != 0)
                        q5cnt[idx] += q5cnt[idx-1];
                        idx ++;
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
        }catch (Exception e) {
            System.out.println("read file error");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Runner.runExample(Main.class);
    }

    class q3Pair implements Comparable{
        long echo;
        String uid;
        String text;
        int impact;
        public q3Pair(long e, String id, String text, int impact) {
            this.echo = e;
            this.uid = id;
            this.text = text;
            this.impact = impact;
        }

        @Override
        public int compareTo(Object o) {
            if ((this.impact == ((q3Pair) o).impact))
                return this.echo - ((q3Pair) o).echo > 0 ? -1 : 1;
            else
                return this.impact - ((q3Pair) o).impact;
        }
    }

    private void init() {
        try {
            // load JDBC
            Class.forName(driver);
            // Get all MySQL clients connected to server
            for (int i = 0; i < max_conn; i++) {
                conns[i] = DriverManager.getConnection(urls[i], username, password);
                if (!conns[i].isClosed())
                    System.out.println("Succeeded connecting to the Database!");
                else
                    System.out.println("Failed in MYSQL conn");
            }
            
        } catch (ClassNotFoundException e) {
            System.out.println("Sorry,can't find the Driver!");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public final String handleQ1(String key, String message) {
        String buffer = "";

        if (key != null && !key.equals("")) {
            BigInteger y = new BigInteger(key).divide(X);
            int z = y.mod(int25).intValue() + 1;
            int matrix_len = (int) Math.sqrt(message.length());

            for (int i = 0; i <= 2 * (matrix_len - 1); i++) {
                int j = i;
                if (j >= matrix_len)
                    j = matrix_len - 1;
                for (; j >= 0; j--) {
                    if ((i - j) >= matrix_len)
                        continue;
                    char ch = message.charAt((i - j) * matrix_len + j);
                    if (ch - z < 'A')
                        buffer += (char)(ch + 26 - z);
                    else
                        buffer += (char)(ch - z);
                }
            }
        }
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
        return timeStamp + "\n" + buffer + "\n";
    }

    public final String handleQ2(String uid, String tweet_time) {
        String res = "";
        try {
            if (uid != null && tweet_time != null) {
                // fetch from MYSQL
                if (isMySQL) {
                    Statement statement = conns[0].createStatement();
                    tweet_time = tweet_time.replace('+', ' ');
                    String key = uid + "+" +tweet_time;
                    for(int i = 0; i < 16-uid.length(); i ++){
                        key = "0" + key;
                    }
                    
                    String sql = "SELECT * FROM q2 USE INDEX (IDX_UID) WHERE uidtime = \'" + key + "\';";
                    ResultSet rs = statement.executeQuery(sql);
                    ArrayList<JSONObject> list = new ArrayList<>();
                    while (rs.next()) {
                        JSONObject obj = new JSONObject();
                        obj.put("tweet_id", rs.getLong("tweet_id"));
                        obj.put("score", rs.getInt("score"));
                        obj.put("text", rs.getString("text"));
                        list.add(obj);
                    }
                    Collections.sort(list, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            return (int)(o1.getLong("tweet_id") - o2.getLong("tweet_id"));
                        }
                    });
                    
                    for (JSONObject obj:list) {
                        res += obj.getLong("tweet_id") + ":";
                        res += obj.getInt("score") + ":";
                        res += obj.getString("text") + "\n";
                    }
                    rs.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }


    public final String handleQ3(String uid, String start_date, String end_date, int cnt) {
        String res = "";
        String content = "";
        //System.out.println(uid);
        try {
            if (uid != null && start_date != null && end_date != null && cnt > 0) {
                
                Statement statement = conns[0].createStatement();
                String sql = "SELECT * FROM q3 USE INDEX (IDX_UID) WHERE user_id = " + uid + ";";
                ResultSet rs = statement.executeQuery(sql);
                while(rs.next()){
                    //System.out.println("hi" + rs.getString("content"));
                    content += rs.getString("content");
                }
                rs.close();
                
                ArrayList<JSONObject> pos = new ArrayList<>();
                ArrayList<JSONObject> neg = new ArrayList<>();
                //System.out.println(content);
                
                
                if(!content.equals("")){
                    JSONArray tweets = new JSONArray(content);
                    
                    for (int i = 0; i < tweets.length(); i++) {
                        JSONObject tweet = tweets.getJSONObject(i);
                        String time = tweet.getString("tweet_time");
                        int score = tweet.getInt("score");
                        if (time.compareTo(start_date) >= 0 && time.compareTo(end_date) <= 0){
                            if(score < 0)
                                neg.add(tweet);
                            else
                                pos.add(tweet);
                        }
                    }
                    
                    Collections.sort(pos, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            if (o2.getInt("score") == o1.getInt("score"))
                                return o1.getString("tweet_id").compareTo(o2.getString("tweet_id"));
                            else
                                return o2.getInt("score") - o1.getInt("score");
                        }
                    });
                    
                    Collections.sort(neg, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            if (o2.getInt("score") == o1.getInt("score"))
                                return o1.getString("tweet_id").compareTo(o2.getString("tweet_id"));
                            else
                                return o1.getInt("score") - o2.getInt("score");
                        }
                    });
                }
                
                res += "Positive Tweets\n";
                for (int i = 0; i < cnt && i < pos.size(); i++) {
                    JSONObject t = pos.get(i);
                    res += t.getString("tweet_time") + ",";
                    res += String.valueOf(t.getInt("score")) + ",";
                    res += t.getString("tweet_id") + ",";
                    res += t.getString("censored_text") + "\n";
                }
                
                res += "\nNegative Tweets\n";
                for (int i = 0; i < cnt && i < neg.size(); i++) {
                    JSONObject t = neg.get(i);
                    res += t.getString("tweet_time") + ",";
                    res += String.valueOf(t.getInt("score")) + ",";
                    res += t.getString("tweet_id") + ",";
                    res += t.getString("censored_text") + "\n";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }


    public final String handleQ4(String hashtag, int cnt) {
        String res = "";
        try {
            if (hashtag != null && cnt != 0) {
                if (isMySQL) {
                    Statement statement = conns[0].createStatement();
                    String sql = "SELECT * FROM q4 USE INDEX (IDX_TAG) WHERE tag = \'" + hashtag
                            + "\' limit "+cnt +";";
                    ResultSet rs = statement.executeQuery(sql);
                    while (rs.next()) {
                        res += rs.getString("date") + ":";
                        res += rs.getInt("count") + ":";
                        res += rs.getString("users") + ":";
                        res += rs.getString("text") + "\n";
                    }
                    rs.close();

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public final String handleQ5(String uid_min, String uid_max) {
        String res = "";
        try {
            if (uid_min != null && uid_max != null) {
                int idmin = (int)(Long.parseLong(uid_min) - Integer.MAX_VALUE);
                int idmax = (int)(Long.parseLong(uid_max) - Integer.MAX_VALUE);
                
                //System.out.println(idmin + ' ' + idmax);
                int left = 0;
                int right = q5uid.length -1;
                int mid, cnt1, cnt2;
                while (left <= right){
                    mid = (left+right)/2;
                    if(q5uid[mid] == idmin){
                        right = mid-1;
                        break;
                    }
                    else if(q5uid[mid] < idmin)
                        left = mid + 1;
                    else
                        right = mid -1;
                }
                if (right < 0)
                    cnt1 = 0;
                else
                    cnt1 = q5cnt[right];
                //System.out.println(q5uid[right]+Integer.MAX_VALUE);
                
                left = 0;
                right = q5uid.length -1;
                //System.out.println(q5uid[right]);
                while (left <= right){
                    mid = (left+right)/2;
                    if(q5uid[mid] == idmax){
                        right = mid;
                        break;
                    }
                    else if(q5uid[mid] < idmax)
                        left = mid + 1;
                    else
                        right = mid -1;
                }
                //System.out.println(right);
                if (right < 0)
                    cnt2 = 0;
                else
                    cnt2 = q5cnt[right];
                
            
            res += cnt2 - cnt1;
            res += '\n';
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public void start() throws Exception {

        init();
        vertx.createHttpServer().requestHandler(req -> executor.submit(() -> {
            // reset result string head
            String result = teamInfo;
            if (req.path().contains("q1")) {
                // parse params
                String key = req.getParam("key");
                String message = req.getParam("message");
                result += handleQ1(key, message);
            }
            else if (req.path().contains("q2")) {
                // parse params
                String uid = req.getParam("userid");
                String tweet_time = req.getParam("tweet_time");
                result += handleQ2(uid, tweet_time);
            }
            else if (req.path().contains("q3")) {
                String uid = req.getParam("userid");
                String start_date = req.getParam("start_date");
                String end_date = req.getParam("end_date");
                int count = Integer.parseInt(req.getParam("n"));
                result += handleQ3(uid, start_date, end_date, count);
            }
            else if (req.path().contains("q4")) {
                String hashtag = req.getParam("hashtag");
                int n = Integer.parseInt(req.getParam("n"));
                result += handleQ4(hashtag, n);
            }
            else if (req.path().contains("q5")) {
                String uid_min = req.getParam("userid_min");
                String uid_max = req.getParam("userid_max");
                result += handleQ5(uid_min, uid_max);
            }
            req.response().putHeader("content-type", "text/html").end(result);
            return null;
        })).listen(80);
    }
}