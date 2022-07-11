-- importing CSVLoader
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load all the data from orders.csv and define the types.
orderCSV= LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVLoader(',') AS
    (
        game_id : int,
        unit_id : int, 
        unit_order : chararray, 
        location : chararray, 
        target : chararray, 
        target_dest : chararray, 
        success : int, 
        reason : chararray, 
        turn_num : int
    );

-- filter by Holland
filtered_data = FILTER orderCSV BY target == 'Holland';

-- Group by location and create the output
grouped_data = FOREACH(GROUP filtered_data by location)
				GENERATE group as location, MAX(filtered_data.(target)) as target, COUNT($1) as c;

-- Sort the data by location ascending
result = ORDER grouped_data BY location ASC;

-- Dump (show) result
DUMP result