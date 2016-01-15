import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: causal, eventual, strong
    private static String consistencyType = "strong";
    private static int[][] delay = {{0, 200, 600}, {200, 0, 800}, {600, 800, 0}};

    /**
        Thread pool executor with a maximum thread number: 10
     */
    private static final int THREAD_NUM = 10;
    private static ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);
    /**
        Thread-safe map: key --> BlockingQueue
        one BlockingQueue for each key
     */
    private static ConcurrentHashMap<String, MyQueue> hashMap = new ConcurrentHashMap();

    /**
        priority blocking queue
     */
    private class MyQueue extends PriorityBlockingQueue<MyTask> {

        public MyQueue() {
            super(1000, new MyComparator());
            // init a handler to poll and submit requests immediately
            new Thread(() -> {
                while (true) {
                    try {
                        executor.submit(MyQueue.this.take()).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-52-91-129-72.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-52-91-49-47.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-52-91-49-251.compute-1.amazonaws.com";
    private static final String[] dataCenters = {dataCenterUSE, dataCenterUSW, dataCenterSING};

    private static final String coordinatorUSE = "ec2-54-172-36-17.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-54-85-200-132.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-54-174-73-74.compute-1.amazonaws.com";
    private static final String[] coordinators = {coordinatorUSE, coordinatorUSW, coordinatorSING};

    class MyComparator implements Comparator<MyTask> {

        @Override
        public int compare(MyTask o1, MyTask o2) {
            return o1.timestamp - o2.timestamp > 0 ? 1:-1;
        }
    }


    abstract class MyTask extends FutureTask<String> {
        public MyTask(Callable<String> c, long t) {
            super(c);
            timestamp = t;
        }
        long timestamp;
    }

    private class GET extends MyTask {
        String key;
        public GET(String k, long ts, Callable<String> c) {
            super(c, ts);
            key = k;
        }
    }

    private class PUT extends MyTask {
        private String key;
        private String value;
        public PUT(String k, long ts, String v, Callable<String> c) {
            super(c, ts);
            this.key = k;
            this.value = v;
        }
    }

    @Override
    public void start() {

        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", req -> {

            MultiMap map = req.params();
            final String key = map.get("key");
            final String value = map.get("value");
            final Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("put: " + key + " timestamp: " + timestamp);
            final String forwarded = map.get("forward");
            final String forwardedRegion = map.get("region");

            if (forwarded == null) {
                System.out.println("sending ahead");
                try {
                    KeyValueLib.AHEAD(key, String.valueOf(timestamp));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("send out ahead");
            }

            if (consistencyType.equals("eventual")) {
                int hash = hashFunc(key);
                System.out.println("Hash result: " + hash);
                // first hand
                if (hash != region - 1) {
                    System.out.println("Forward to " + hash);
                    try {
                        KeyValueLib.FORWARD(coordinators[hash], key, value, String.valueOf(timestamp - delay[region-1][hash]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        System.out.println("PUT ALL: " + key + " Timestamp: " + timestamp);
                        // blocks
                        KeyValueLib.PUT(dataCenters[0], key, value, String.valueOf(timestamp - delay[region-1][0]), consistencyType);
                        System.out.println("PUT 1 complete");
                        KeyValueLib.PUT(dataCenters[1], key, value, String.valueOf(timestamp - delay[region-1][1]), consistencyType);
                        System.out.println("PUT 2 complete");
                        KeyValueLib.PUT(dataCenters[2], key, value, String.valueOf(timestamp - delay[region-1][2]), consistencyType);
                        System.out.println("PUT 3 complete");
                        KeyValueLib.COMPLETE(key, String.valueOf(timestamp));
                        System.out.println("All complete");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            } else {
                MyTask task;
                task = new PUT(key, timestamp, value, () -> {
                    int hash = hashFunc(key);
                    System.out.println("Hash result: " + hash);
                    // first hand
                    if (hash != region - 1) {
                        System.out.println("Forward to " + hash);
                        KeyValueLib.FORWARD(coordinators[hash], key, value, String.valueOf(timestamp - delay[region-1][hash]));
                    } else {
                        try {
                            System.out.println("PUT ALL: " + key + " Timestamp: " + timestamp);
                            // blocks
                            KeyValueLib.PUT(dataCenters[0], key, value, String.valueOf(timestamp - delay[region-1][0]), consistencyType);
                            System.out.println("PUT 1 complete");
                            KeyValueLib.PUT(dataCenters[1], key, value, String.valueOf(timestamp - delay[region-1][1]), consistencyType);
                            System.out.println("PUT 2 complete");
                            KeyValueLib.PUT(dataCenters[2], key, value, String.valueOf(timestamp - delay[region-1][2]), consistencyType);
                            System.out.println("PUT 3 complete");
                            KeyValueLib.COMPLETE(key, String.valueOf(timestamp));
                            System.out.println("All complete");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    return null;
                });
                // put it into priority queue
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
            }

            req.response().end(); // Do not remove this
        });

        routeMatcher.get("/get", req -> {

            MultiMap map = req.params();
            final String key = map.get("key");
            final Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("get: " + key + " Timestamp: " + timestamp);
            String response = "";
            // init a GET task
            if (consistencyType.equals("strong")) {
                MyTask task;
                task = new GET(key, timestamp, () -> KeyValueLib.GET(dataCenters[region - 1], key, String.valueOf(timestamp), consistencyType));
                // put into the queue
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
                    response = task.get();
                    System.out.println("Get Timestamp: " + timestamp + " response: " + response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    response = KeyValueLib.GET(dataCenters[region - 1], key, String.valueOf(timestamp), consistencyType);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            req.response().end(response);
        });
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", req -> {
            MultiMap map = req.params();
            consistencyType = map.get("consistency");
            req.response().end();
        });
        /* BONUS HANDLERS BELOW */
        routeMatcher.get("/forwardcount", req -> {
            req.response().end(KeyValueLib.COUNT());
        });

        routeMatcher.get("/reset", req -> {
            KeyValueLib.RESET();
            hashMap.clear();
            req.response().end();
        });

        routeMatcher.noMatch(req -> {
            req.response().putHeader("Content-Type", "text/html");
            String response = "Not found.";
            req.response().putHeader("Content-Length",
                    String.valueOf(response.length()));
            req.response().end(response);
            req.response().close();
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }

    /**
        sum up all characters, mod 3
     */
    private int hashFunc(String key) {
        int result = 0;
        for (int i = 0; i < key.length(); i++) {
            result += key.charAt(i) - 'a';
        }
        return Math.abs(result % 3);
    }
}
