-- View: public.v_trips

DROP MATERIALIZED VIEW public.v_trips;

CREATE MATERIALIZED VIEW public.v_trips
TABLESPACE pg_default
AS
 SELECT trips.provider_id,
    trips.trip_id,
    trips.provider_name,
    trips.device_id,
    trips.vehicle_id,
    trips.vehicle_type,
    trips.propulsion_type,
    trips.trip_duration,
    trips.trip_distance,
    trips.trip_distance::numeric * 0.000621371192 AS trip_distance_miles,
    trips.route as route_json,
    trips.start_time,
    timezone('PST'::text, trips.start_time) AS start_time_local,
    trips.end_time,
    timezone('PST'::text, trips.end_time) AS end_time_local,
    trips.parking_verification_url,
    trips.standard_cost,
    trips.actual_cost,
	trips_geoms.points as trip_geometry, 
	ST_StartPoint(trip_geometry) as start_point,
	ST_EndPoint(trip_geometry) as end_point
   FROM trips INNER JOIN trips_geoms 
   ON
		trips.trip_id = trips_geoms.trip_id
WITH NO DATA;
 
ALTER TABLE public.v_trips
    OWNER TO dbadmin;

GRANT SELECT ON TABLE public.v_trips TO dot_mony_ro;
GRANT SELECT ON TABLE public.v_trips TO dot_paul_ro;
GRANT ALL ON TABLE public.v_trips TO dbadmin;
GRANT SELECT ON TABLE public.v_trips TO dot_vlad_ro;


DROP MATERIALIZED VIEW public.v_status_changes;

CREATE MATERIALIZED VIEW public.v_status_changes
TABLESPACE pg_default
AS
 SELECT status_changes.provider_id,
    status_changes.provider_name,
    status_changes.device_id,
    status_changes.vehicle_id,
	status_changes.vehicle_type,
	status_changes.propulsion_type,
    status_changes.event_type,
    status_changes.event_type_reason,
    status_changes.event_time,
	status_changes.battery_pct,
	status_changes.associated_trips,
	status_changes.id,
    status_changes.event_location as event_location_json,
    timezone('PST'::text, status_changes.event_time) AS event_time_local, 
	status_change_geoms.event_location_geom as event_location_geom,
   FROM status_changes INNER JOIN status_change_geoms
WITH NO DATA;

ALTER TABLE public.v_status_changes
    OWNER TO dbadmin;

GRANT SELECT ON TABLE public.v_status_changes TO dot_mony_ro;
GRANT SELECT ON TABLE public.v_status_changes TO dot_paul_ro;
GRANT ALL ON TABLE public.v_status_changes TO dbadmin;
GRANT SELECT ON TABLE public.v_status_changes TO dot_vlad_ro;