import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
    private HashMap<String, ArrayList<StoreValue>> store = null;
    /**
        Thread pool executor with a maximum thread number: 10
     */
    private static final int THREAD_NUM = 10;
    private static ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);

    private class MyQueue extends PriorityBlockingQueue<DCTask> {
        String key;
        public MyQueue(String key) {
            super(1000, new MyComparator());
            // init a handler to poll and submit requests immediately
            this.key = key;
            new Thread(() -> {
                while (true) {
                    try {
                        /*
                        if(MyQueue.this.isEmpty())
                            continue;
                        DCTask new_task = MyQueue.this.peek();
                        if(lockMap.containsKey(key) && !lockMap.get(key).isEmpty()
                                && lockMap.get(key).peek() < new_task.timestamp)
                            continue;*/
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
        Thread-safe map: key --> BlockingQueue
        one BlockingQueue for each key
     */
    private static ConcurrentHashMap<String, MyQueue> hashMap = new ConcurrentHashMap();
    private static ConcurrentHashMap<String, PriorityQueue<Long>> lockMap = new ConcurrentHashMap();

    public KeyValueStore() {
        store = new HashMap<String, ArrayList<StoreValue>>();
    }

    class MyComparator implements Comparator {

        @Override
        public int compare(Object o1, Object o2) {
            return ((DCTask) o1).timestamp - ((DCTask) o2).timestamp > 0 ? 1:-1;
        }
    }

    abstract class DCTask extends FutureTask<String> {
        long timestamp;

        public DCTask(long ts, Callable<String> callable) {
            super(callable);
            this.timestamp = ts;
        }
    }

    private class LocalGET extends DCTask {
        String key;
        long timestamp;
        public LocalGET(String k, long ts, Callable<String> callable) {
            super(ts, callable);
            this.key = k;
        }
    }

    private class LocalPUT extends DCTask {
        String key;
        String value;
        long timestamp;
        public LocalPUT(String k, String v, long ts, Callable<String> callable) {
            super(ts, callable);
            this.key = k;
            this.value = v;
        }

    }


    @Override
    public void start() {
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", req -> {
            System.out.println("put dc");
            MultiMap map = req.params();
            String key = map.get("key");
            String value = map.get("value");
            String consistency = map.get("consistency");
            Integer region = Integer.parseInt(map.get("region"));

            Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("put: " + key + " timestamp: " + timestamp);

            /* TODO: You will need to adjust the timestamp here for some consistency levels */
            if (consistency.equals("eventual")) {
                if (store.containsKey(key))
                    store.get(key).add(new StoreValue(timestamp, value));
                else {
                    ArrayList<StoreValue> list = new ArrayList();
                    list.add(new StoreValue(timestamp, value));
                    store.put(key, list);
                }
            } else {
                DCTask task;
                task = new LocalPUT(key, value, timestamp, () -> {

                    System.out.println("start to put " + key);
                    if (store.containsKey(key))
                        store.get(key).add(new StoreValue(timestamp, value));
                    else {
                        ArrayList<StoreValue> list = new ArrayList();
                        list.add(new StoreValue(timestamp, value));
                        store.put(key, list);
                    }
                    return null;
                });
                // if already exists
                if (hashMap.containsKey(key)) {
                    hashMap.get(key).offer(task);
                }
                // else init a queue
                else {
                    MyQueue queue = new MyQueue(key);
                    queue.offer(task);
                    hashMap.put(key, queue);
                }
                // being blocked

                try {
                    task.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            String response;
            response = "stored";
            req.response().putHeader("Content-Type", "text/plain");
            req.response().putHeader("Content-Length",
                    String.valueOf(response.length()));
            req.response().end(response);
            req.response().close();
        });

        routeMatcher.get("/get", req -> {

            MultiMap map = req.params();
            final String key = map.get("key");
            String consistency = map.get("consistency");
            final Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("get: " + key + " timestamp: " + timestamp);
            /* TODO: Add code here to get the list of StoreValue associated with the key
             * Remember that you may need to implement some locking on certain consistency levels */

            /* Do NOT change the format the response. It will return a string of
             * values separated by spaces */

            String response = "";
            if (consistency.equals("strong")) {
                DCTask task;
                task = new LocalGET(key, timestamp, () -> {

                    String result = "";
                    if (store.get(key) != null) {
                        for (StoreValue sv : store.get(key)) {
                            result += sv.getValue() + " ";
                        }
                    }
                    return result;
                });

                if (hashMap.containsKey(key)) {
                    hashMap.get(key).offer(task);
                }
                // else init a queue
                else {
                    MyQueue queue = new MyQueue(key);
                    queue.offer(task);
                    hashMap.put(key, queue);
                }
                // wait to get result
                try {
                    response = task.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                if (store.get(key) != null) {
                    for (StoreValue sv : store.get(key)) {
                        response += sv.getValue() + " ";
                    }
                }
            }

            req.response().putHeader("Content-Type", "text/plain");
            if (response != null)
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
            req.response().end(response);
            req.response().close();
        });

        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", req -> {
            System.out.println("ahead dc");
            MultiMap map = req.params();
            String key = map.get("key");
            final Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("ahead: " + key + " timestamp: " + timestamp);

            if (lockMap.containsKey(key))
                lockMap.get(key).offer(timestamp);
            else {
                PriorityQueue<Long> q = new PriorityQueue<Long>();
                q.offer(timestamp);
                lockMap.put(key, q);
            }

            req.response().putHeader("Content-Type", "text/plain");
            req.response().end();
            req.response().close();
        });

        // Handler for when the COMPLETE is called
        routeMatcher.get("/complete", req -> {
            System.out.println("complete dc");
            MultiMap map = req.params();
            String key = map.get("key");
            final Long timestamp = Long.parseLong(map.get("timestamp"));
            System.out.println("complete: " + key + " timestamp: " + timestamp);
            lockMap.get(key).poll();
            req.response().putHeader("Content-Type", "text/plain");
            req.response().end();
            req.response().close();
        });

        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", req -> {
            store.clear();
            hashMap.clear();
            lockMap.clear();
            System.out.println("RESET");
            req.response().putHeader("Content-Type", "text/plain");
            req.response().end();
            req.response().close();
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
}
