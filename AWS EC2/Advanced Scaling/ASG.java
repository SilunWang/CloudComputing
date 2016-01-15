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
import com.amazonaws.services.securitytoken.model.Credentials;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Silun Wang on 15/10/01.
 */

public class ASG {

    // control clients
    static AmazonEC2Client ec2Client;
    static AmazonElasticLoadBalancingClient elbClient;
    static AmazonAutoScalingClient asgClient;
    static AmazonCloudWatchClient cloudWatchClient;
    static BasicAWSCredentials credentials;
    // static strings
    static String propertyFileName = "AWSCredentials.properties";
    static String LGSecurityGroupName = "LG-Security";
    static String ASGSecurityGroupName = "ASG-Security";
    static String us_east_1d_subnetID = "subnet-3bc58e10";
    static String ELBName = "ASG-LoadBalancer";
    static String ASGName = "ASG";
    static String LaunchConfigName = "ASG-Launch-Configuration";
    static String scaleUpPolicyName = "scale-up";
    static String scaleDownPolicyName = "scale-down";
    static String scaleUpAlarmName = "Scale-up";
    static String scaleDownAlarmName = "Scale-down";

    /*
        get the public dns of an instance
        please use this function instead of instance.getPublicDnsName()
     */
    static String getInstancePublicDnsName(String instanceId) {
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

    static void sendHttpRequest(String url) {
        try {
            URL mURL = new URL(url);
            URLConnection load_gen_conn = mURL.openConnection();
            // print http response
            InputStream is = load_gen_conn.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            while((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void initAWSClients() {
        ec2Client = new AmazonEC2Client(credentials);
        elbClient = new AmazonElasticLoadBalancingClient(credentials);
        asgClient = new AmazonAutoScalingClient(credentials);
        cloudWatchClient = new AmazonCloudWatchClient(credentials);
    }

    public static void main(String[] args) {
        // read property file
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertyFileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
            credentials
         */
        credentials = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));

        initAWSClients();

        /*
            create tags request
         */

        // for load generator
        ArrayList<com.amazonaws.services.ec2.model.Tag> requestTags = new ArrayList<com.amazonaws.services.ec2.model.Tag>();
        requestTags.add(new com.amazonaws.services.ec2.model.Tag("Project", "2.2"));
        CreateTagsRequest tagsRequest = new CreateTagsRequest();
        tagsRequest.setTags(requestTags);

        // for load balancer
        ArrayList<com.amazonaws.services.elasticloadbalancing.model.Tag> lbTags = new ArrayList<com.amazonaws.services.elasticloadbalancing.model.Tag>();
        com.amazonaws.services.elasticloadbalancing.model.Tag lbTag = new com.amazonaws.services.elasticloadbalancing.model.Tag();
        lbTag.setKey("Project");
        lbTag.setValue("2.2");
        lbTags.add(lbTag);

        // for ASG
        ArrayList<com.amazonaws.services.autoscaling.model.Tag> asgTags = new ArrayList<com.amazonaws.services.autoscaling.model.Tag>();
        com.amazonaws.services.autoscaling.model.Tag asgTag = new com.amazonaws.services.autoscaling.model.Tag();
        asgTag.setKey("Project");
        asgTag.setValue("2.2");
        asgTags.add(asgTag);

        /*
            create security group request
         */
        IpPermission ipPermission = new IpPermission();
        ipPermission.withIpRanges("0.0.0.0/0").withIpProtocol("-1");

        // load generator's security group
        CreateSecurityGroupRequest LG_Security_Group = new CreateSecurityGroupRequest();
        LG_Security_Group.withGroupName(LGSecurityGroupName).withDescription("For load generator");
        AuthorizeSecurityGroupIngressRequest LG_authorize_request =
                new AuthorizeSecurityGroupIngressRequest();
        LG_authorize_request.withGroupName(LGSecurityGroupName).withIpPermissions(ipPermission);

        // auto-scaling group's security group
        CreateSecurityGroupRequest ASG_Security_Group = new CreateSecurityGroupRequest();
        ASG_Security_Group.withGroupName(ASGSecurityGroupName).withDescription("For auto-scaling group");
        AuthorizeSecurityGroupIngressRequest ASG_authorize_request =
                new AuthorizeSecurityGroupIngressRequest();
        ASG_authorize_request.withGroupName(ASGSecurityGroupName).withIpPermissions(ipPermission);

        /*
            ec2Client init & add tags & add securityGroup
         */

        ec2Client.createSecurityGroup(LG_Security_Group);
        ec2Client.createSecurityGroup(ASG_Security_Group);
        ec2Client.authorizeSecurityGroupIngress(LG_authorize_request);
        ec2Client.authorizeSecurityGroupIngress(ASG_authorize_request);
        // get security group id
        String LG_SecurityGroupId = "";
        ArrayList<String> ASG_SecurityGroup = new ArrayList<String>();
        DescribeSecurityGroupsRequest dsgr = new DescribeSecurityGroupsRequest();
        DescribeSecurityGroupsResult securityGroupsResult = ec2Client.describeSecurityGroups(dsgr);
        for ( SecurityGroup sg : securityGroupsResult.getSecurityGroups()) {
            if (sg.getGroupName().equals(LGSecurityGroupName)) {
                LG_SecurityGroupId = sg.getGroupId();
                break;
            } else if (sg.getGroupName().equals(ASGSecurityGroupName)) {
                ASG_SecurityGroup.add(sg.getGroupId());
            }
        }

        /*
            set up load generator
         */

        RunInstancesRequest LGInstancesRequest = new RunInstancesRequest();
        LGInstancesRequest.withImageId("ami-312b5154")
                .withInstanceType("m3.medium")
                .withMinCount(1)
                .withMaxCount(1)
                .withSecurityGroupIds(LG_SecurityGroupId)
                .withSubnetId(us_east_1d_subnetID);

        /*
            run instance: Load Generator
         */

        RunInstancesResult LGInstancesResult = ec2Client.runInstances(LGInstancesRequest);
        Instance loadGenerator = LGInstancesResult.getReservation().getInstances().get(0);
        tagsRequest.withResources(loadGenerator.getInstanceId()).withTags(requestTags);
        ec2Client.createTags(tagsRequest);


        String load_gen_dns = "";

        try {
            System.out.println("LG Launched. Please wait ...");
            Thread.sleep(60000);
            load_gen_dns = getInstancePublicDnsName(loadGenerator.getInstanceId());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // create load balancer
        List<Listener> listeners = new ArrayList<Listener>(1);
        listeners.add(new Listener("HTTP", 80, 80));
        CreateLoadBalancerRequest lbRequest = new CreateLoadBalancerRequest();
        lbRequest.setLoadBalancerName(ELBName);
        lbRequest.setTags(lbTags);
        lbRequest.setSecurityGroups(ASG_SecurityGroup);
        lbRequest.setListeners(listeners);
        ArrayList<String> subnets = new ArrayList<String>();
        subnets.add(us_east_1d_subnetID);
        lbRequest.setSubnets(subnets);

        // set health check
        HealthCheck healthCK = new HealthCheck()
                .withHealthyThreshold(2)
                .withUnhealthyThreshold(6)
                .withInterval(30)
                .withTarget("HTTP:80/heartbeat?lg=" + load_gen_dns)
                .withTimeout(5);
        // health check configure request
        ConfigureHealthCheckRequest healthCheckReq = new ConfigureHealthCheckRequest()
                .withHealthCheck(healthCK)
                .withLoadBalancerName(ELBName);

        // submit request
        elbClient.createLoadBalancer(lbRequest);
        elbClient.configureHealthCheck(healthCheckReq);
        System.out.println("created load balancer loader");

        CreateLaunchConfigurationRequest lcRequest = new CreateLaunchConfigurationRequest();
        lcRequest.setLaunchConfigurationName(LaunchConfigName);
        lcRequest.setImageId("ami-3b2b515e");
        lcRequest.setSecurityGroups(ASG_SecurityGroup);
        lcRequest.setInstanceType("m3.medium");


        CreateAutoScalingGroupRequest asg_create_request = new CreateAutoScalingGroupRequest();

        asgClient.createLaunchConfiguration(lcRequest);

        asg_create_request.setTags(asgTags);
        asg_create_request.setAutoScalingGroupName("ASG");
        asg_create_request.setMinSize(2);
        asg_create_request.setMaxSize(10);
        asg_create_request.setDefaultCooldown(60);
        ArrayList<String> lbList = new ArrayList<String>();
        lbList.add(ELBName);
        asg_create_request.setLoadBalancerNames(lbList);
        asg_create_request.setHealthCheckType("ELB");
        asg_create_request.setHealthCheckGracePeriod(119);
        asg_create_request.setLaunchConfigurationName(LaunchConfigName);
        ArrayList<String> avZones = new ArrayList<String>();
        avZones.add("us-east-1d");
        asg_create_request.setAvailabilityZones(avZones);
        asgClient.createAutoScalingGroup(asg_create_request);


        /*
            Scale Policy
         */

        // scale up request
        PutScalingPolicyRequest up_request = new PutScalingPolicyRequest();
        up_request.setAutoScalingGroupName(ASGName);
        up_request.setPolicyName(scaleUpPolicyName);
        up_request.setScalingAdjustment(2); // scale up by one
        up_request.setAdjustmentType("ChangeInCapacity");
        up_request.setCooldown(60);
        // return result
        PutScalingPolicyResult result = asgClient.putScalingPolicy(up_request);
        String arn = result.getPolicyARN();
        // Scale Up Policy
        PutMetricAlarmRequest upRequest = new PutMetricAlarmRequest();
        upRequest.setAlarmName(scaleUpAlarmName);
        upRequest.setMetricName("CPUUtilization");
        upRequest.setNamespace("AWS/EC2");
        upRequest.setComparisonOperator(ComparisonOperator.GreaterThanThreshold);
        upRequest.setStatistic(Statistic.Average);
        upRequest.setUnit(StandardUnit.Percent);
        upRequest.setThreshold(70d);
        upRequest.setPeriod(60);
        upRequest.setEvaluationPeriods(1);


        ArrayList<String> actions = new ArrayList<String>();
        actions.add(arn); // This is the value returned by the ScalingPolicy request
        upRequest.setAlarmActions(actions);

        // Scale Down Policy
        // scale down request
        PutScalingPolicyRequest down_request = new PutScalingPolicyRequest();
        down_request.setAutoScalingGroupName(ASGName);
        down_request.setPolicyName(scaleDownPolicyName); // This scales up so I've put up at the end.
        down_request.setScalingAdjustment(-2); // scale down by one
        down_request.setAdjustmentType("ChangeInCapacity");
        down_request.setCooldown(60);
        // scale down result
        PutScalingPolicyResult result2 = asgClient.putScalingPolicy(down_request);
        String arn2 = result2.getPolicyARN(); // You need the policy ARN in the next step so make a note of it.
        // Scale Up Policy
        PutMetricAlarmRequest downRequest = new PutMetricAlarmRequest();
        downRequest.setAlarmName(scaleDownAlarmName);
        downRequest.setMetricName("CPUUtilization");
        downRequest.setNamespace("AWS/EC2");
        downRequest.setComparisonOperator(ComparisonOperator.LessThanThreshold);
        downRequest.setStatistic(Statistic.Average);
        downRequest.setUnit(StandardUnit.Percent);
        downRequest.setThreshold(30d);
        downRequest.setPeriod(60);
        downRequest.setEvaluationPeriods(1);

        ArrayList<String> actions2 = new ArrayList<String>();
        actions2.add(arn2); // This is the value returned by the ScalingPolicy request
        downRequest.setAlarmActions(actions2);

        // Cloud Watch
        cloudWatchClient.putMetricAlarm(upRequest);
        cloudWatchClient.putMetricAlarm(downRequest);

        // submit password
        try {
            System.out.println("ASG Launched. Please wait 10min ...");
            Thread.sleep(600000);
            String password = properties.getProperty("password");
            // send http request
            sendHttpRequest("http://" + load_gen_dns + "/password?passwd=" + password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // warm up
        try {
            String elb_dns = elbClient.describeLoadBalancers().
                    getLoadBalancerDescriptions().get(0).getDNSName();
            // send http request
            sendHttpRequest("http://" + load_gen_dns + "/warmup?dns=" + elb_dns);
            System.out.println("Warmup test Launched. Please wait for 5min ...");
            Thread.sleep(330000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // junior test
        try {
            // send http request
            String elb_dns = elbClient.describeLoadBalancers().
                    getLoadBalancerDescriptions().get(0).getDNSName();
            sendHttpRequest("http://" + load_gen_dns + "/junior?dns=" + elb_dns);
            // wait junior complete
            System.out.println("please wait for junior test to complete");
            Thread.sleep(2900000);
        } catch (Exception e) {
            e.printStackTrace();
        }


        // clean up
        cleanUpMess();
        System.out.println("END");
    }

    static void cleanUpMess() {
        try {
            // delete scaling policy
            DeletePolicyRequest deletePolicyRequest = new DeletePolicyRequest();
            deletePolicyRequest.setAutoScalingGroupName(ASGName);
            deletePolicyRequest.setPolicyName(scaleUpPolicyName);
            asgClient.deletePolicy(deletePolicyRequest);
            deletePolicyRequest.setPolicyName(scaleDownPolicyName);
            asgClient.deletePolicy(deletePolicyRequest);

            // delete cloud watch alarm
            DeleteAlarmsRequest deleteAlarmsRequest = new DeleteAlarmsRequest();
            ArrayList<String> alarmNames = new ArrayList<String>();
            alarmNames.add(scaleUpAlarmName);
            alarmNames.add(scaleDownAlarmName);
            deleteAlarmsRequest.setAlarmNames(alarmNames);
            cloudWatchClient.deleteAlarms(deleteAlarmsRequest);

            // delete load balancer
            DeleteLoadBalancerRequest deleteLoadBalancerRequest = new DeleteLoadBalancerRequest();
            deleteLoadBalancerRequest.setLoadBalancerName(ELBName);
            elbClient.deleteLoadBalancer(deleteLoadBalancerRequest);

            // delete ASG
            UpdateAutoScalingGroupRequest updateAutoScalingGroupRequest = new UpdateAutoScalingGroupRequest();
            updateAutoScalingGroupRequest.setAutoScalingGroupName(ASGName);
            updateAutoScalingGroupRequest.setMinSize(0);
            updateAutoScalingGroupRequest.setMaxSize(0);
            asgClient.updateAutoScalingGroup(updateAutoScalingGroupRequest);
            System.out.println("waiting for instances to be terminated...");
            Thread.sleep(30000);
            DeleteAutoScalingGroupRequest deleteAutoScalingGroupRequest = new DeleteAutoScalingGroupRequest();
            deleteAutoScalingGroupRequest.setAutoScalingGroupName(ASGName);
            asgClient.deleteAutoScalingGroup(deleteAutoScalingGroupRequest);

            // delete launch configuration
            DeleteLaunchConfigurationRequest deleteLaunchConfigurationRequest = new DeleteLaunchConfigurationRequest();
            deleteLaunchConfigurationRequest.setLaunchConfigurationName(LaunchConfigName);
            asgClient.deleteLaunchConfiguration(deleteLaunchConfigurationRequest);

            // delete security group
            DeleteSecurityGroupRequest deleteSecurityGroupRequest = new DeleteSecurityGroupRequest();
            deleteSecurityGroupRequest.setGroupName(ASGSecurityGroupName);
            ec2Client.deleteSecurityGroup(deleteSecurityGroupRequest);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
