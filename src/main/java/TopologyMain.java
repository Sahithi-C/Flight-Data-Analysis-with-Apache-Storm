import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import main.java.bolts.AirlineSorter;
import main.java.bolts.HubIdentifier;
import main.java.spouts.FlightsDataReader;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        // Validate command line arguments
        if (args.length != 2) {
            System.out.println("Usage: java TopologyMain <flights_file> <airports_file>");
            System.exit(1);
        }
        
        // Create topology builder
        TopologyBuilder builder = new TopologyBuilder();
        
        // Configure the spout (FlightsDataReader) with a single-task
        builder.setSpout("flights-reader", new FlightsDataReader(), 1);
        
        // Configure the first bolt (HubIdentifier) with 2 parallel tasks and Shuffle grouping from spout to first bolt
        builder.setBolt("hub-identifier", new HubIdentifier(), 2)
            .shuffleGrouping("flights-reader");
        
        // Configure the second bolt (AirlineSorter) with a single-task
        // Field grouping from first bolt to second bolt on "airport.city"
        builder.setBolt("airline-sorter", new AirlineSorter(), 1)
            .fieldsGrouping("hub-identifier", new Fields("airport.city"));
        
        Config conf = new Config();
        conf.put("FlightsFile", args[0]);
        conf.put("AirportsData", args[1]);
        conf.setDebug(false);
        
        // Running topology locally
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("FlightAnalysisTopology", conf, builder.createTopology());
            // Run for 10 seconds
            Thread.sleep(10000); 
        } finally {
            cluster.shutdown();
        }
    }
}