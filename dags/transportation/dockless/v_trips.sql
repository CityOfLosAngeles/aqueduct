-- View: mds.v_trips

DROP IF EXISTS MATERIALIZED VIEW mds.v_trips CASCADE;

CREATE MATERIALIZED VIEW mds.v_trips
TABLESPACE pg_default
AS
 SELECT mt.provider_id,
    mt.trip_id,
    mt.provider_name,
    mt.device_id,
    mt.vehicle_id,
    mt.vehicle_type,
    mt.propulsion_type,
    mt.trip_duration,
    mt.trip_distance,
    mt.trip_distance::numeric * 0.000621371192 AS trip_distance_miles,
    mt.route as route_json,
    mt.start_time,
    timezone('PST'::text, mt.start_time) AS start_time_local,
    mt.end_time,
    timezone('PST'::text, mt.end_time) AS end_time_local,
    mt.parking_verification_url,
    mt.standard_cost,
    mt.actual_cost,
	trips_geoms.points as trip_geometry,
	ST_StartPoint(trips_geoms.points) as start_point,
	ST_EndPoint(trips_geoms.points) as end_point
   FROM mds.trips mt INNER JOIN trips_geoms
   ON
		mt.trip_id = trips_geoms.trip_id
WITH DATA;

CREATE INDEX idx_trips_start_time ON mds.v_trips(start_time_local);
CREATE INDEX idx_trips_end_time ON mds.v_trips(end_time_local);

ALTER TABLE mds.v_trips
    OWNER TO dbadmin;

GRANT SELECT ON TABLE mds.v_trips TO dot_mony_ro;
GRANT SELECT ON TABLE mds.v_trips TO dot_paul_ro;
GRANT ALL ON TABLE mds.v_trips TO dbadmin;
GRANT SELECT ON TABLE mds.v_trips TO dot_vlad_ro;

-- view mds.v_status_changes

DROP IF EXISTS MATERIALIZED VIEW mds.v_status_changes CASCADE;

CREATE MATERIALIZED VIEW mds.v_status_changes
TABLESPACE pg_default
AS
 SELECT msc.provider_id,
    msc.provider_name,
    msc.device_id,
    msc.vehicle_id,
    msc.vehicle_type,
    msc.propulsion_type,
    msc.event_type,
    msc.event_type_reason,
    msc.event_time,
    msc.battery_pct,
    msc.associated_trips,
    msc.id,
    msc.event_location as event_location_json,
    timezone('PST'::text, msc.event_time) AS event_time_local,
	status_change_geoms.event_location_geom as event_location_geom
   FROM mds.status_changes msc INNER JOIN status_change_geoms
   ON msc.id = status_change_geoms.status_change_id
WITH DATA;

CREATE INDEX idx_status_change_event_time ON mds.v_status_changes(event_time_local);

ALTER TABLE mds.v_status_changes
    OWNER TO dbadmin;

GRANT SELECT ON TABLE mds.v_status_changes TO dot_mony_ro;
GRANT SELECT ON TABLE mds.v_status_changes TO dot_paul_ro;
GRANT ALL ON TABLE mds.v_status_changes TO dbadmin;
GRANT SELECT ON TABLE mds.v_status_changes TO dot_vlad_ro;

--- make some indexes

CREATE INDEX idx_status_change_geometry
    ON mds.v_status_changes USING gist
    (event_location_geom)

CREATE INDEX idx_trip_start
    ON mds.v_trips USING gist
    (start_point);

CREATE INDEX idx_trip_end
    ON mds.v_trips USING gist
    (end_point);
