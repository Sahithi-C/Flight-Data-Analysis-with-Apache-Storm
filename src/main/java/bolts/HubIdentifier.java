package main.java.bolts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HubIdentifier extends BaseBasicBolt {
    private List<Airport> airports = new ArrayList<>();
    
    // Inner class to represent Airport data
    public static class Airport implements Serializable {
        private static final long serialVersionUID = 1L;
        
        String city;
        String code;
        double latitude;
        double longitude;
        
        public Airport(String city, String code, double latitude, double longitude) {
            this.city = city;
            this.code = code;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }
    
    // Cleanup method to release any resources when bolt finishes execution
    @Override
    public void cleanup() {
        // No resources to clean up 
    }
    
    // Prepare method to load airports data from the provided file
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // Read the airports data file
        String airportsFile = stormConf.get("AirportsData").toString();
        loadAirportsData(airportsFile);
    }
    
    // Declare the output fields that this bolt will emit
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport.city", "airport.code", "call sign"));
    }
    
    // Process each incoming tuple (flight data)
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            // Get flight data from input tuple
            String callSign = input.getStringByField("call sign").trim();
            String latitudeStr = input.getStringByField("latitude");
            String longitudeStr = input.getStringByField("longitude");
            
            // Skip if latitude or longitude is empty
            if (latitudeStr.isEmpty() || longitudeStr.isEmpty()) {
                return;
            }
            
            double flightLatitude = Double.parseDouble(latitudeStr);
            double flightLongitude = Double.parseDouble(longitudeStr);
            
            // Check if flight is near any airport (within 20 miles)
            for (Airport airport : airports) {
                if (isFlightNearAirport(flightLatitude, flightLongitude, airport)) {
                    // Emit the flight data with the corresponding airport
                    collector.emit(new Values(airport.city, airport.code, callSign));
                    // found a match, no need to check other airports
                    break;
                }
            }
        } catch (Exception e) {
            // Skip this flight if there's an error processing it
            System.err.println("Error processing flight: " + e.getMessage());
        }
    }
    
    // Utility method to load airports data from file
    private void loadAirportsData(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    String city = parts[0];
                    String code = parts[1];
                    double latitude = Double.parseDouble(parts[2]);
                    double longitude = Double.parseDouble(parts[3]);
                    
                    airports.add(new Airport(city, code, latitude, longitude));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading airports file: " + fileName, e);
        }
    }
    
    // Check if a flight is within 20 miles of an airport
    private boolean isFlightNearAirport(double flightLat, double flightLong, Airport airport) {
        // 1 degree in latitude = 70 miles
        // 1 degree in longitude = 45 miles
        // Calculating 20 miles in terms of latitude and longitude
        final double TWENTY_MILES_LAT = 20.0 / 70.0;  // ~0.2857 degrees
        final double TWENTY_MILES_LONG = 20.0 / 45.0; // ~0.4444 degrees
        
        // Check if the flight is within 20 miles of the airport in either direction
        boolean withinLatitude = Math.abs(flightLat - airport.latitude) <= TWENTY_MILES_LAT;
        boolean withinLongitude = Math.abs(flightLong - airport.longitude) <= TWENTY_MILES_LONG;
        
        // The flight is considered near if it's within 20 miles in either latitude or longitude
        return withinLatitude || withinLongitude;
    }
}