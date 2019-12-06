-- View: mds.v_trips

DROP MATERIALIZED VIEW mds.v_trips CASCADE;

CREATE MATERIALIZED VIEW mds.v_trips
TABLESPACE pg_default
AS
 SELECT mt.trips.provider_id,
    mt.trips.trip_id,
    mt.trips.provider_name,
    mt.trips.device_id,
    mt.trips.vehicle_id,
    mt.trips.vehicle_type,
    mt.trips.propulsion_type,
    mt.trips.trip_duration,
    mt.trips.trip_distance,
    mt.trips.trip_distance::numeric * 0.000621371192 AS trip_distance_miles,
    mt.trips.route as route_json,
    mt.trips.start_time,
    timezone('PST'::text, mt.trips.start_time) AS start_time_local,
    mt.trips.end_time,
    timezone('PST'::text, mt.trips.end_time) AS end_time_local,
    mt.trips.parking_verification_url,
    mt.trips.standard_cost,
    mt.trips.actual_cost,
	trips_geoms.points as trip_geometry,
	ST_StartPoint(trips_geoms.points) as start_point,
	ST_EndPoint(trips_geoms.points) as end_point
   FROM mds.trips mt INNER JOIN trips_geoms
   ON
		mt.trips.trip_id = trips_geoms.trip_id
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

DROP MATERIALIZED VIEW mds.v_status_changes CASCADE;

CREATE MATERIALIZED VIEW mds.v_status_changes
TABLESPACE pg_default
AS
 SELECT msc.status_changes.provider_id,
    msc.status_changes.provider_name,
    msc.status_changes.device_id,
    msc.status_changes.vehicle_id,
	msc.status_changes.vehicle_type,
	msc.status_changes.propulsion_type,
    msc.status_changes.event_type,
    msc.status_changes.event_type_reason,
    msc.status_changes.event_time,
	msc.status_changes.battery_pct,
	msc.status_changes.associated_trips,
	msc.status_changes.id,
    msc.status_changes.event_location as event_location_json,
    timezone('PST'::text, msc.status_changes.event_time) AS event_time_local,
	status_change_geoms.event_location_geom as event_location_geom
   FROM mds.status_changes msc INNER JOIN status_change_geoms
   ON msc.status_changes.id = status_change_geoms.status_change_id
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
