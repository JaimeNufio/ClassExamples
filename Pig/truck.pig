truck_events = LOAD '/input/truck_event_text_partition.csv' USING PigStorage(',');
DESCRIBE truck_events;
