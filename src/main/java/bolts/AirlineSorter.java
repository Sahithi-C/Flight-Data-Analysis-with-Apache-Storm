package main.java.bolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class AirlineSorter extends BaseBasicBolt {
    private Integer id;
    private String name;
    // Map <airport_code, <airline_code, count>>
    private Map<String, Map<String, Integer>> airportFlights;
    // Map to store airport names by <airport_code, airport_city>
    private Map<String, String> airportNames;
    
    @Override
    public void cleanup() {
        System.out.println(" -- Flight Counter [" + name + "-" + id + "] -- ");
        
        // Process each airport's data
        for (Entry<String, Map<String, Integer>> airport : airportFlights.entrySet()) {
            String airportCode = airport.getKey();
            String airportCity = airportNames.get(airportCode);
            Map<String, Integer> airlines = airport.getValue();
            
            // Print airport header
            System.out.println(" At Airpot: " + airportCode + "(" + airportCity + ")");
            
            // Get total flights for this airport
            int totalFlights = 0;
            for (Integer count : airlines.values()) {
                totalFlights += count;
            }
            
            // Sort airlines by number of flights (descending order)
            List<Map.Entry<String, Integer>> sortedAirlines = new ArrayList<>(airlines.entrySet());
            Collections.sort(sortedAirlines, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue()); // Descending order
                }
            });
            
            // Print each airline and flight count
            for (Map.Entry<String, Integer> airline : sortedAirlines) {
                System.out.println("\t" + airline.getKey() + ": " + airline.getValue());
            }
            
            // Print total flights
            System.out.println("\ttotal #flights = " + totalFlights);
            System.out.println();
        }
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.airportFlights = new HashMap<>();
        this.airportNames = new HashMap<>();
    }
    
    // Declare the output fields this bolt will emit
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields 
    }
    
    // Process each incoming tuple and sorts airlines by the number of flights
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String airportCity = input.getStringByField("airport.city");
        String airportCode = input.getStringByField("airport.code");
        String callSign = input.getStringByField("call sign").trim();
        
        // Extract airline code by first 3 characters of call sign
        String airlineCode = callSign.length() >= 3 ? callSign.substring(0, 3) : callSign;
        
        // Store the airport name
        airportNames.put(airportCode, airportCity);
        
        // Get or create the map for this airport
        Map<String, Integer> airlines = airportFlights.get(airportCode);
        if (airlines == null) {
            airlines = new HashMap<>();
            airportFlights.put(airportCode, airlines);
        }
        
        // Update the count for this airline
        Integer count = airlines.get(airlineCode);
        if (count == null) {
            airlines.put(airlineCode, 1);
        } else {
            airlines.put(airlineCode, count + 1);
        }
    }
}