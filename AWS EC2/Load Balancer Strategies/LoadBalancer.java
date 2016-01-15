import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Random;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.Timer;
import java.util.TimerTask;
import java.io.FileInputStream;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.*;
import com.amazonaws.services.ec2.model.Tag;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.lang.Thread;

public class LoadBalancer {
    private static final int THREAD_POOL_SIZE = 4;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    // unhealthy count times
    private int unhealthyCnt[] = new int[3];
    // unhealth state
    private boolean unhealthy[] = new boolean[3];
    private BasicAWSCredentials credentials;
    private AmazonEC2Client ec2Client;
    // the instances being launced & not ready
    private HashSet<Integer> launchingSet = new HashSet<Integer>();

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
        this.socket = socket;
        this.instances = instances;
        // read property file
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // init credentials
        credentials = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));
        // init aws ec2 controller
        ec2Client = new AmazonEC2Client(credentials);
    }

    // check if http response is 200
    public boolean checkHttpResponse(String url) {
        try {
            URL murl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) murl.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            int code = connection.getResponseCode();
            if (code != 200)
                return false;
            else
                return true;
        } catch (Exception e) {
            System.out.println("unhealthy response");
            return false;
        }
    }

    // get an instance's public DNS according to its id
    public String getInstancePublicDnsName(String instanceId) {
        DescribeInstancesResult describeInstancesRequest = ec2Client.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();
        for (Reservation reservation : reservations) {
            for (Instance instance : reservation.getInstances()) {
                if (instance.getInstanceId().equals(instanceId))
                    return instance.getPublicDnsName();
            }
        }
        return null;
    }

    // start a new Data Center Instance
    public Instance startNewDCInstance() {
        ArrayList<Tag> requestTags = new ArrayList<Tag>();
        requestTags.add(new Tag("Project", "2.3"));
        CreateTagsRequest tagsRequest = new CreateTagsRequest();
        tagsRequest.setTags(requestTags);
        RunInstancesRequest instancesRequest = new RunInstancesRequest();
        instancesRequest.withImageId("ami-ed80c388")
                .withInstanceType("m3.medium")
                .withMinCount(1)
                .withMaxCount(1)
                .withSecurityGroupIds("sg-bbaa4cdd")
                .withSubnetId("subnet-ba1531e3");
        RunInstancesResult instancesResult = ec2Client.runInstances(instancesRequest);
        Instance dataCenterInstance = instancesResult.getReservation().getInstances().get(0);
        tagsRequest.withResources(dataCenterInstance.getInstanceId()).withTags(requestTags);
        ec2Client.createTags(tagsRequest);
        return dataCenterInstance;
    }

    public void healthCheck() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask(){

            @Override
            public void run() {

                for (int i = 0; i < instances.length; i++) {
                    String url = instances[i].getUrl();
                    // if url still not set
                    if (url.equals("http://")) {
                        url = "http://" + getInstancePublicDnsName(instances[i].getId());
                        instances[i].setUrl(url);
                        System.out.println(url);
                    }
                    // check health status
                    boolean ok = checkHttpResponse(url);
                    if (ok) {
                        unhealthyCnt[i] = 0;
                        unhealthy[i] = false;
                        if (launchingSet.contains(i)) {
                            launchingSet.remove(i);
                        }
                    } else {
                        if (unhealthyCnt[i] < 3)
                            unhealthyCnt[i]++;
                        // unhealthy state and not yet launch a new DC
                        if (unhealthyCnt[i] >= 3 && !launchingSet.contains(i)) {
                            unhealthy[i] = true;
                            System.out.println("ready to launch new ins");
                            Instance DCI = startNewDCInstance();
                            String instanceId = DCI.getInstanceId();
                            String newurl = "http://" + getInstancePublicDnsName(instanceId);
                            instances[i] = new DataCenterInstance(instances[i].getName(), newurl, instanceId);
                            System.out.println(instanceId);
                            unhealthyCnt[i] = 0;
                            launchingSet.add(i);
                        }
                    }
                }
                
            }
        };
        // 2 mins from now
        long delay = 120000;
        // 5 seconds interval
        long period = 2000;
        // start schedule
        timer.schedule(task, delay, period);
    }

    // Complete this function
    public void start() throws IOException {

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        /*
            code for health check test and senior test
         */
        healthCheck();

        while (true) {
            for (int i = 0; i < 3; i++) {
                if (!unhealthy[i]) {
                    Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                    executorService.execute(requestHandler);
                }
            }
        }
        /*
            code for round robin test
         */
        /*
        while (true) {
            for (int i = 0; i < 3; i++) {
                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                executorService.execute(requestHandler);
            }
        }*/

        /*
            code for customized test
         */
        /*
        while (true) {
            Random rand = new Random();
            int n = rand.nextInt(3);
            Runnable requestHandler = new RequestHandler(socket.accept(), instances[n]);
            executorService.execute(requestHandler);
        }*/
    }
}
