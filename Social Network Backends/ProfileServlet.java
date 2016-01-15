package cc.cmu.edu.minisite;
/**
 * Created by Silun Wang on 11/3/15.
 */
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {

    Connection conn;

    public ProfileServlet() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/project34?useUnicode=true&characterEncoding=utf-8";
        String user = "root";
        String password = "15319project";

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if(!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");

        } catch(ClassNotFoundException e) {
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        String name = "Unauthorized";
        String profile = "#";

        try {
            Statement statement1 = conn.createStatement();
            Statement statement2 = conn.createStatement();
            String usersSQL = "SELECT * FROM users WHERE id = " + id + " AND password = \'" + pwd + "\';";
            ResultSet usersResult = statement1.executeQuery(usersSQL);
            //System.out.println(usersSQL);
            if (usersResult.next()) {
                //System.out.println("1");
                String userinfoSQL = "SELECT * FROM userinfo WHERE id = " + id + " ;";
                ResultSet userinfoResult = statement2.executeQuery(userinfoSQL);
                if (userinfoResult.next()) {
                    //System.out.println("2");
                    name = userinfoResult.getString("name");
                    profile = userinfoResult.getString("profile");
                }
                userinfoResult.close();
            }
            usersResult.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        result.put("name", name);
        result.put("profile", profile);

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
