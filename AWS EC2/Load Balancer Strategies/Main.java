import java.io.IOException;
import java.net.ServerSocket;

public class Main {
	private static final int PORT = 80;
	private static DataCenterInstance[] instances;
	private static ServerSocket serverSocket;

	//Update this list with the DNS of your data center instances
	static {
		instances = new DataCenterInstance[3];
		instances[0] = new DataCenterInstance("first_instance", "http://ec2-54-152-123-215.compute-1.amazonaws.com", "i-40e2e8e0");
		instances[1] = new DataCenterInstance("second_instance", "http://ec2-54-152-162-154.compute-1.amazonaws.com", "i-7fe1ebdf");
		instances[2] = new DataCenterInstance("third_instance", "http://ec2-52-91-207-224.compute-1.amazonaws.com", "i-81e6ec21");
	}

	public static void main(String[] args) throws IOException {
		initServerSocket();
		LoadBalancer loadBalancer = new LoadBalancer(serverSocket, instances);
		loadBalancer.start();
	}

	/**
	 * Initialize the socket on which the Load Balancer will receive requests from the Load Generator
	 */
	private static void initServerSocket() {
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (IOException e) {
			System.err.println("ERROR: Could not listen on port: " + PORT);
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
