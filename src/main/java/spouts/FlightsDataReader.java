package main.java.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FlightsDataReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    
    // Acknowledge when a tuple is successfully processed
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }
    
    // Cleanup when the spout is closed
    public void close() {}
    
    // Prints message when a tuple fails to process
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }
    
    // Emits tuples with flight data to the next components in the topology
    @Override
    public void nextTuple() {
        if (completed) {
            // Sleep if the file has already been processed
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Do nothing, ignore interruption
            }
            return;
        }
        
        BufferedReader reader = new BufferedReader(fileReader);
        String line;
        
        try {
            // Read file line by line
            while ((line = reader.readLine()) != null) {
                // Skip JSON syntax lines that don't contain flight data
                if (line.contains("{") || line.contains("}") || 
                    line.contains("[") || line.contains("]") || 
                    line.trim().length() == 0 || 
                    line.contains("\"states\":") || 
                    line.contains("\"time\":")) {
                    continue;
                }
                
                // Process flight data and emit if valid
                String[] flightData = parseFlightData(line);
                if (flightData != null && flightData.length == 17) {
                    this.collector.emit(new Values(
                        flightData[0],  // transponder address
                        flightData[1],  // call sign
                        flightData[2],  // origin country
                        flightData[3],  // last timestamp 1
                        flightData[4],  // last timestamp 2
                        flightData[5],  // longitude
                        flightData[6],  // latitude
                        flightData[7],  // altitude (barometric)
                        flightData[8],  // surface or air
                        flightData[9],  // velocity
                        flightData[10], // degree north = 0
                        flightData[11], // vertical rate
                        flightData[12], // sensors
                        flightData[13], // altitude (geometric)
                        flightData[14], // transponder code
                        flightData[15], // special purpose
                        flightData[16]  // origin
                    ), line);
                }
            }
        } catch (Exception e) {
            // Throw error if something is wrong
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            // Mark as completed and close the file reader
            completed = true;
            try {
                reader.close();
            } catch (IOException e) {
                // Do nothing, ignore exception while closing
            }
        }
    }
    
    // Initialize the spout and prepare resources
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("FlightsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("FlightsFile") + "]", e);
        }
        // Set collector for emitting tuples
        this.collector = collector;
    }
    
    // Declare the output fields emitted by this spout
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
            "transponder address", "call sign", "origin country", 
            "last timestamp", "last timestamp2", "longitude", 
            "latitude", "altitude (barometric)", "surface or air", 
            "velocity (meters/sec)", "degree north = 0", "vertical rate", 
            "sensors", "altitude (geometric)", "transponder code", 
            "special purpose", "origin"
        ));
    }
    
    // Utility method to parse flight data from JSON line
    private String[] parseFlightData(String line) {
        line = line.trim();
        
        // Remove commas and brackets
        if (line.endsWith(",")) {
            line = line.substring(0, line.length() - 1);
        }
        
        // Extract the array content
        int startIdx = line.indexOf("[");
        int endIdx = line.lastIndexOf("]");
        
        if (startIdx != -1 && endIdx != -1) {
            line = line.substring(startIdx + 1, endIdx);
            
            // Split by commas, but respect quoted strings
            String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            
            // Clean up each part
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
                
                // Remove quotes
                if (parts[i].startsWith("\"") && parts[i].endsWith("\"")) {
                    parts[i] = parts[i].substring(1, parts[i].length() - 1);
                }
                
                // Convert null to empty string
                if (parts[i].equals("null")) {
                    parts[i] = "";
                }
            }
            
            return parts;
        }
        
        return null;
    }
}