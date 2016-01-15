package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

  /* Define per task state here. (kv stores etc) */
  private KeyValueStore<String, String> driverLoc;
  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
  //Initialize stuff (maybe the kv stores?)
    driverLoc = (KeyValueStore<String, String>) context.getStore("driver-loc");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
  // The main part of your code. Remember that all the messages for a particular partition
  // come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
  // at one task only, thereby enabling you to do stateful stream processing.
    String incomingStream = envelope.getSystemStreamPartition().getStream();
    if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
      processDriverLocation((Map<String, Object>) envelope.getMessage());
    } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
      processEvent((Map<String, Object>) envelope.getMessage(), collector);
    } else {
      throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
    }
  }

  private void processDriverLocation(Map<String, Object> message) {
      String driver_id = String.valueOf(message.get("driverId"));
      String block_id = String.valueOf(message.get("blockId"));
      String latitude = String.valueOf(message.get("latitude"));
      String longitude = String.valueOf(message.get("longitude"));
      driverLoc.put(block_id + "-" + driver_id, longitude + "-" + latitude);
  }

  private void processEvent(Map<String, Object> message, MessageCollector collector) {
    try {
      // common values
      String type = (String) message.get("type");
      String block_id = String.valueOf(message.get("blockId"));
      String longitude = String.valueOf(message.get("longitude"));
      String latitude = String.valueOf(message.get("latitude"));

      // leaving this block: delete from map
      if (type.equals("LEAVING_BLOCK")) {
        String driver_id = String.valueOf(message.get("driverId"));
        String key = block_id + "-" + driver_id;
        if (driverLoc.get(key) != null)
          driverLoc.delete(key);
      }
      // entering this block
      else if (type.equals("ENTERING_BLOCK")) {
        String driver_id = String.valueOf(message.get("driverId"));
        String key = block_id + "-" + driver_id;
        // available
        if (message.get("status").equals("AVAILABLE"))
          driverLoc.put(key, longitude + "-" + latitude);
      }
      // someone needs a drive
      else if (type.equals("RIDE_REQUEST")) {
        KeyValueIterator<String, String> iterator = driverLoc.range(block_id, String.valueOf(Integer.parseInt(block_id) + 1));
        int a1 = Integer.parseInt(longitude);
        int b1 = Integer.parseInt(latitude);
        double miniDist = Double.MAX_VALUE;
        String matchDriver = "1";
        // retrieve nearest driver
        while (iterator.hasNext()) {
          Entry<String, String> entry = iterator.next();
          //System.out.println(entry.getKey());
          String driver_id = entry.getKey().split("-")[1];
          String[] arr = entry.getValue().split("-");
          int a2 = Integer.parseInt(arr[0]);
          int b2 = Integer.parseInt(arr[1]);
          double dist = Math.sqrt((a1 - a2) * (a1 - a2) + (b1 - b2) * (b1 - b2));
          if (dist < miniDist) {
            miniDist = dist;
            matchDriver = driver_id;
          }
        }
        String rider_id = String.valueOf(message.get("riderId"));
        String result = String.format("{\"riderId\":%s, \"driverId\":%s}", String.valueOf(message.get("riderId")), matchDriver);
        message.clear();
        message.put("riderId", rider_id);
        message.put("driverId", matchDriver);
        // send to output stream
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
        System.out.println(result);
        // this driver is unavailable
        driverLoc.delete(block_id + "-" + matchDriver);
        iterator.close();
      }
      // driver is available now
      else if (type.equals("RIDE_COMPLETE")) {
        String driver_id = String.valueOf(message.get("driverId"));
        String key = block_id + "-" + driver_id;
        driverLoc.put(key, longitude + "-" + latitude);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
  //this function is called at regular intervals, not required for this project
  }
}
