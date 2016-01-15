import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.Dimension;
import java.io.FileInputStream;
import java.util.*;
import java.util.Properties;

public class DataCenterInstance {
	private final String name;
	private String url;
	private final String instanceId;
	private BasicAWSCredentials credentials;

	public DataCenterInstance(String name, String url, String id) {
		this.name = name;
		this.url = url;
		this.instanceId = id;
		Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        credentials = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));
	}

	public String getName() {
		return name;
	}

	public String getUrl() {
		return url;
	}

    public String getId() {
        return instanceId;
    }

    public void setUrl(String murl) {
        this.url = murl;
    }

    // get CPU util statistics: average and max
	public double getLatestCPUUtil() {
		AmazonCloudWatchClient cw = new AmazonCloudWatchClient(credentials) ;
        long offsetInMilliseconds = 1000 * 60 * 30;
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                .withStartTime(new Date(new Date().getTime() - offsetInMilliseconds))
                .withNamespace("AWS/EC2")
                .withPeriod(60)
                .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
                .withMetricName("CPUUtilization")
                .withStatistics("Average", "Maximum")
                .withEndTime(new Date());

        GetMetricStatisticsResult getMetricStatisticsResult = cw.getMetricStatistics(request);
        int len = getMetricStatisticsResult.getDatapoints().size();
        double avr = getMetricStatisticsResult.getDatapoints().get(len - 1).getAverage();
        return avr;
	}

	/**
	 * Execute the request on the Data Center Instance
	 * @param path
	 * @return URLConnection
	 * @throws IOException
	 */
	public URLConnection executeRequest(String path) throws IOException {
		URLConnection conn = openConnection(path);
		return conn;
	}

	/**
	 * Open a connection with the Data Center Instance
	 * @param path
	 * @return URLConnection
	 * @throws IOException
	 */
	private URLConnection openConnection(String path) throws IOException {
		URL url = new URL(path);
		URLConnection conn = url.openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(false);
		return conn;
	}
}
