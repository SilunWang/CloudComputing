import java.util.*;
import java.sql.Timestamp;
import java.util.concurrent.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

/*
	Author: Silun Wang
	andrew id: silunw
 */

public class Coordinator extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";
	private static final int THREAD_NUM = 10;

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-152-238-111.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-52-91-91-205.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-52-91-95-136.compute-1.amazonaws.com";
	private static final String[] dataCenters = {dataCenter1, dataCenter2, dataCenter3};
	/*
		Thread pool executor with a maximum thread number: 10
	 */
	private static ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);
	/*
		Thread-safe map: key --> BlockingQueue
		one BlockingQueue for each key
	 */
	private static ConcurrentHashMap<String, MyQueue> hashMap = new ConcurrentHashMap();

	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis()
						+ TimeZone.getTimeZone("EST").getRawOffset()).toString();

				//task is a runnable
				FutureTask<String> task;
				if (storageType.equals("sharding"))
					task = new FutureTask<String>(new ShardingPUT(key, value));
				else
					task = new FutureTask<String>(new ReplicationPUT(key, value));
				// if queue already exists
				if (hashMap.containsKey(key)) {
					hashMap.get(key).offer(task);
				}
				// else init a queue
				else {
					MyQueue queue = new MyQueue();
					queue.offer(task);
					hashMap.put(key, queue);
				}

				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis()
						+ TimeZone.getTimeZone("EST").getRawOffset()).toString();

				String resultStr = "";
				FutureTask<String> task;

				if (storageType.equals("sharding"))
					task = new FutureTask<String>(new ShardingGET(key, loc));
				else
					task = new FutureTask<String>(new ReplicationGET(key, loc));
				// if the queue does not exists
				if (hashMap.containsKey(key)) {
					hashMap.get(key).offer(task);
				}
				// else init a queue
				else {
					MyQueue queue = new MyQueue();
					queue.offer(task);
					hashMap.put(key, queue);
				}
				// wait to get result
				try {
					resultStr = task.get();
				} catch (Exception e) {
					e.printStackTrace();
				}

				req.response().end(resultStr);
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				storageType = map.get("storage");
				System.out.println(storageType);
				//This endpoint will be used by the auto-grader to set the
				//consistency type that your key-value store has to support.
				//You can initialize/re-initialize the required data structures here
				req.response().end();
			}
		});

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});

		server.requestHandler(routeMatcher);
		server.listen(8080);
	}


	/*
		PUT function for Replication
	 */
	private class ReplicationPUT implements Callable<String> {
		private String key;
		private String value;

		public ReplicationPUT(String key, String value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String call() throws Exception {
			KeyValueLib.PUT(dataCenter1, this.key, this.value);
			KeyValueLib.PUT(dataCenter2, this.key, this.value);
			KeyValueLib.PUT(dataCenter3, this.key, this.value);
			return null;
		}
	}

	/*
		GET function for Replication
	 */
	private class ReplicationGET implements Callable<String> {
		private String key;
		private String loc;

		public ReplicationGET(String key, String loc) {
			this.key = key;
			this.loc = loc;
		}

		@Override
		public String call() throws Exception {
			String dns = dataCenters[Integer.parseInt(this.loc) - 1];
			return KeyValueLib.GET(dns, key);
		}
	}

	/*
		PUT function for Sharding
	 */
	private class ShardingPUT implements Callable<String> {
		private String key;
		private String value;

		public ShardingPUT(String key, String value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String call() throws Exception {
			int hash = hashFunc(this.key);
			KeyValueLib.PUT(dataCenters[hash], this.key, this.value);
			return null;
		}
	}

	/*
		GET function for Replication
	 */
	private class ShardingGET implements Callable<String> {
		private String key;
		private String loc;

		public ShardingGET(String key, String loc) {
			this.key = key;
			this.loc = loc;
		}

		@Override
		public String call() throws Exception {
			int hash = hashFunc(this.key);
			if (!loc.equals(String.valueOf(hash + 1)))
				return "0";
			else
				return KeyValueLib.GET(dataCenters[hash], key);
		}
	}

	/*
		sum up all characters, mod 3
	 */
	private int hashFunc(String key) {
		int result = 0;
		for (int i = 0; i < key.length(); i++) {
			result += key.charAt(i) - 'a';
		}
		return Math.abs(result % 3);
	}

	/*
		Linked list queue
	 */
	private class MyQueue extends LinkedBlockingQueue<Runnable> {

		public MyQueue() {

			super();
			// init a handler to poll and submit requests immediately
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							executor.submit(MyQueue.this.take()).get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
	}
}

