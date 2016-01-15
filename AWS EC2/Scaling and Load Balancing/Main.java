import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import java.io.*;
import java.net.*;
import java.util.*;


/**
 * Created by Silun on 15/9/23.
 */

public class Main {

    static AmazonEC2Client ec2;

    /*
        get the public dns of an instance
        please use this function instead of instance.getPublicDnsName()
     */
    static String getInstancePublicDnsName(String instanceId) {
        DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();
        for (Reservation reservation : reservations) {
            for (Instance instance : reservation.getInstances()) {
                if (instance.getInstanceId().equals(instanceId))
                    return instance.getPublicDnsName();
            }
        }
        return null;
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("AWSCredentials.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
            credentials
         */
        BasicAWSCredentials credentials = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));
        /*
            create tags request
         */
        ArrayList<Tag> requestTags = new ArrayList<Tag>();
        requestTags.add(new Tag("Project", "2.1"));
        CreateTagsRequest tagsRequest = new CreateTagsRequest();
        tagsRequest.setTags(requestTags);

        /*
            create security group request
         */
        CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
        csgr.withGroupName("All-Traffics").withDescription("Allowing all traffic");
        IpPermission ipPermission = new IpPermission();
        ipPermission.withIpRanges("0.0.0.0/0").withIpProtocol("-1");
        AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =
                new AuthorizeSecurityGroupIngressRequest();
        authorizeSecurityGroupIngressRequest.withGroupName("All-Traffics")
                .withIpPermissions(ipPermission);

        /*
            ec2 init & add tags & add securityGroup
         */
        ec2 = new AmazonEC2Client(credentials);
        ec2.createSecurityGroup(csgr);
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
        String securityGroupId = "";
        DescribeSecurityGroupsRequest dsgr = new DescribeSecurityGroupsRequest();
        DescribeSecurityGroupsResult securityGroupsResult = ec2.describeSecurityGroups(dsgr);
        for ( SecurityGroup sg : securityGroupsResult.getSecurityGroups()) {
            if (sg.getGroupName().equals("All-Traffics")) {
                securityGroupId = sg.getGroupId();
                break;
            }
        }

        /*
            load generator
         */
        RunInstancesRequest LGInstancesRequest = new RunInstancesRequest();
        LGInstancesRequest.withImageId("ami-4389fb26")
                .withInstanceType("m3.medium")
                .withMinCount(1)
                .withMaxCount(1)
                .withSecurityGroupIds(securityGroupId)
                .withSubnetId("subnet-3bc58e10");
        /*
            data center
         */
        RunInstancesRequest DCInstancesRequest = new RunInstancesRequest();
        DCInstancesRequest.withImageId("ami-abb8cace")
                .withInstanceType("m3.medium")
                .withMinCount(1)
                .withMaxCount(1)
                .withSecurityGroupIds(securityGroupId)
                .withSubnetId("subnet-3bc58e10");
        /*
            run instance: Load Generator
         */
        RunInstancesResult LGInstancesResult = ec2.runInstances(LGInstancesRequest);
        Instance loadGenerator = LGInstancesResult
                .getReservation().getInstances().get(0);
        tagsRequest.withResources(loadGenerator.getInstanceId())
                .withTags(requestTags);
        ec2.createTags(tagsRequest);

        String load_gen_dns = "";
        try {
            System.out.println("Launched. Please wait 120s...");
            Thread.sleep(120000);
            load_gen_dns = getInstancePublicDnsName(loadGenerator.getInstanceId());
            String password = properties.getProperty("password");
            // send http request
            URL load_gen_url =
                    new URL("http://" + load_gen_dns + "/password?passwd=" + password);
            URLConnection load_gen_conn = load_gen_url.openConnection();
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
        /*
            run instance: Data Center
         */
        ArrayList<Instance> DCs = new ArrayList<Instance>();
        RunInstancesResult DCInstancesResult = ec2.runInstances(DCInstancesRequest);
        Instance dataCenter = DCInstancesResult.getReservation().getInstances().get(0);
        DCs.add(dataCenter);
        tagsRequest.withResources(dataCenter.getInstanceId())
                .withTags(requestTags);
        ec2.createTags(tagsRequest);

        try {
            System.out.println("Data center launched. Please wait 120s...");
            Thread.sleep(120000);
            String data_center_dns = getInstancePublicDnsName(dataCenter.getInstanceId());
            // send http request
            URL data_center_url =
                    new URL("http://" + load_gen_dns + "/test/horizontal?dns=" + data_center_dns);
            URLConnection data_center_conn = data_center_url.openConnection();
            // read http response
            InputStream is = data_center_conn.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            while((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*
            create new data centers
         */
        System.out.println("Create a new data center? (y/N): ");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            String userInput = bufferedReader.readLine();
            while (userInput.toLowerCase().equals("y")) {
                RunInstancesResult newDCInstancesResult =
                        ec2.runInstances(DCInstancesRequest);
                Instance newDataCenter =
                        newDCInstancesResult.getReservation().getInstances().get(0);
                DCs.add(newDataCenter);
                tagsRequest.withResources(newDataCenter.getInstanceId())
                        .withTags(requestTags);
                ec2.createTags(tagsRequest);

                System.out.println("Data center launched. Please wait for 120s.");

                Thread.sleep(120000);
                String data_center_dns = getInstancePublicDnsName(newDataCenter.getInstanceId());
                URL data_center_url =
                        new URL("http://" + load_gen_dns + "/test/horizontal/add?dns=" + data_center_dns);
                URLConnection data_center_conn = data_center_url.openConnection();

                InputStream is = data_center_conn.getInputStream();
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line;
                while((line = rd.readLine()) != null) {
                    System.out.println(line);
                }
                rd.close();

                System.out.println("Create a new data center? (y/N): ");
                userInput = bufferedReader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
