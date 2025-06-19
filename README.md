# Flight Data Analysis with Apache Storm

This project practices distributed data streaming and analysis, using Apache Storm. As an example dataset, I used the Open Air Traffic Data for Research website (at https://opensky-network.org/) and count the number of flights departing from or arriving at each US major airport per airline company.  

## Input and Output Data:
**Input files:** There are two input files under the src/main/resources/ directory.
**(1) flights.txt:** a worldwide flight dataset obtained from https://opensky-network.org/. To verify the program, I used this text file. Playing with real-time flight data, by executing the get_flights_data.sh under the same directory. It just includes a curl command: 
curl -s "https://opensky-network.org/api/states/all" | python -m json.tool > flights.txt 

This JSON-formatted file includes a list of flights, each with 17 data fields 
'''
{ 
    "states": [ 
        [ 
            "a09281", 
            "N136LM  ", 
            "United States", 
            1551555425, 
            1551555503, 
            -84.7205, 
            33.9124, 
            883.92, 
            false, 
            58.13, 
            270, 
            -1.3, 
            null, 
            891.54, 
            "1200", 
            true, 
            0 
        ], 
        [ 
            ... 
        ], 
    ], 
    "time": 1551555579 
} 
'''
 **(2) aiports.txt:** a dataset of the top 40 US airports, including each airport’s city name, IATA code, latitude and longitude. This text file is used for running the program.

'''  
Atlanta,ATL,33.6367,-84.4281 
Los Angeles,LAX,33.9425,-118.4081 
Dallas-Fort Worth,DFW,32.8969,-97.0381 
...
'''

## System.out messages: 
This program reads flights.txt, choose only flights flying closer to any of the top 40 airports, (assuming that they just took off from or are landing at the airport rather than passing it over), and sort them out per each airline company. The expected output is: 

 '''
"transponder address",  // [0] 
"call sign",            // [1] 
"origin country",       // [2] 
"last timestamp",       // [3] 
"last timestamp",       // [4] 
"longitude",            // [5] 
"latitude",             // [6] 
"altitude (barometric)",// [7] 
"surface or air",       // [8] 
"velocity (meters/sec)",// [9] 
"degree north = 0",     // [10] 
"vertical rate",        // [11] 
"sensors",              // [12] 
"altitude (geometric)", // [13] 
"transponder code",     // [14] 
"special purpose",      // [15] 
"origin"                // [16] 
 -- Flight Counter [airline-sorter-7] -- 
 At Airpot: ORD(Chicago) 
         LOT: 1 
         UAL: 5 
         RPA: 1 
         AIJ: 1 
         AAL: 2 
         SWA: 2 
         N21: 1 
         EJA: 3 
         SKW: 2 
         DAL: 1 
         ASQ: 1 
         total #flights = 20 
 
 At Airpot: SAN(San Diego) 
         VOI: 1 
         UAL: 1 
         FFT: 1 
         N28: 1 
         RCH: 1 
         N95: 1 
         AAL: 3 
         SWA: 2 
         SKW: 3 
         DAL: 1 
         total #flights = 15 
 
 ...
 '''

## Documentation:
**1. TopologyMain.java:** This is the main program which connects all components together into the processing pipeline. It manages the data flows through the execution of the topology. It creates a processing pipeline consisting of one spout and two bolts. This takes two files as input, one with flight data and one with airport locations. The topology reads flight data and airport location information from files provided and connects components using shuffle grouping between the spout and first bolt, and routes flights based on their destination city between the first and second bolt. The topology runs locally for 10 seconds before shutting down.

**2. FlightsDataReader.java:** This program reads flight information and emits data to the topology. It parses each line of flight data while also filtering out JSON syntax lines which don't contain actual flight data, extracting 17 flight attributes like transponder address, call sign, origin country, coordinates, altitude, velocity, and heading information. It handles file reading operations and error conditions, emitting valid flight records as tuples, skipping invalid entries. The emitted data is passed to the HubIdentifier bolt for further processing.

**3. HubIdentifier.java:** This program checks if flights are near airports. It loads airport data like city, code, latitude, longitude and processes incoming flight records. For each flight with valid coordinates, it checks if the flight is within 20 miles of the airport. The distance is calculated based on latitude and longitude differences. When a flight is identified near an airport, it emits the airport city, airport code, and flight call sign to the next component. This bolt identifies which flights are operating near major transportation hubs.

**4. AirlineSorter.java:** This program takes the airport and flight information from the previous part, counts and organizes flight information by airline and airport. It gets the airline code from the first three letters of each flight's call sign and keeps track of how many flights from each airline are near each airport. When the topology completes, it shows a list of all airports where for each airport, it shows which airlines have the most flights nearby. It sorts the airlines from most flights to least flights and shows the total number of flights for each airport.

One additional feature of the program is the AirlineSorter bolt sorts airlines by the number of flights in descending order, making it easier to identify the most active airlines at each airport. It also sorts and groups flight data by both airport and airline. This is especially useful in real-time monitoring. 

## Limitations and Possible Improvements:
However, while the output is informative, it can compute more advanced statistics such as delays, average flight counts per airline, or comparisons between airports. Another limitation is that additional flight parameters like vertical rate and altitude could be considered to more accurately identify flights landing or taking off from airports.
