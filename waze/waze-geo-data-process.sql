--waze-geo-data-process.sql
-- after adding data using old method (not storing geometry) run this to
-- Backfill geometry into geoTable from location data in AJI tables.
-- can take many hours

-- alerts have a single point
WITH
 alertgeom AS (
    SELECT pid,
           ST_MakeLine(
             ST_MakePoint(
               (location ->> 'x')::NUMERIC,
               (location ->> 'y')::NUMERIC
             )
           ) AS geom
    FROM waze.alerts
    GROUP BY pid
  )

INSERT INTO waze.geoTable(alert_id,geom)
SELECT b.pid,ST_SetSRID(b.geom, 4326)
FROM alertgeom AS b;

-- jams/irregularities use multipoint lines
WITH
 irreglines AS (
    SELECT pid,
           ST_MakeLine(
             ST_MakePoint(
               (line -> n ->> 'x')::NUMERIC,
               (line -> n ->> 'y')::NUMERIC
             )
           ) AS geom
    FROM waze.irregularities
    CROSS JOIN generate_series(0, jsonb_array_length(line)) AS n
    GROUP BY pid
  )

INSERT INTO waze.geoTable(irregularity_id,geom)
SELECT b.pid,ST_SetSRID(b.geom, 4326)
FROM irreglines AS b;

WITH
 jamlines AS (
    SELECT pid,
           ST_MakeLine(
             ST_MakePoint(
               (line -> n ->> 'x')::NUMERIC,
               (line -> n ->> 'y')::NUMERIC
             )
           ) AS geom
    FROM waze.jams
    CROSS JOIN generate_series(0, jsonb_array_length(line)) AS n
    GROUP BY pid
  )

INSERT INTO waze.geoTable(jam_id,geom)
SELECT b.pid,ST_SetSRID(b.geom, 4326)
FROM jamlines AS b;
