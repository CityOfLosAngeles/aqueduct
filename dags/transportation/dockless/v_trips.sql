CREATE MATERIALIZED VIEW v_trips AS 

SELECT provider_id,
	   provider_name, 
	   device_id,
	   vehicle_id,
	   vehicle_type,
	   propulsion_type,
	   trip_duration, 
	   trip_distance, 
	   trip_distance * 0.000621371192 as trip_distance_miles,
	   route, 
	   start_time, 
	   (start_time AT TIME ZONE 'UTC') AT TIME ZONE 'PST' as start_time_local,
	   end_time,
	   (end_time) AT TIME ZONE 'UTC' AT TIME ZONE 'PST'  AS end_time_local,
	   parking_verification_url,
	   standard_cost,
	   actual_cost
FROM trips
WITH NO DATA; 