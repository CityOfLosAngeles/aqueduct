-- waze-geo-trigger.sql
-- create a trigger which adds geometry to waze.geoTable on insert into a/j/i

---- ALERTS (location:point)
CREATE OR REPLACE FUNCTION waze.fn_geom_on_insert_alert() RETURNS TRIGGER AS
$BODY$
BEGIN
	INSERT INTO waze.geoTable(alert_id,geom)
	SELECT NEW.pid,
			ST_SetSRID(
				ST_MakeLine(
		             ST_MakePoint(
		               (NEW.location ->> 'x')::NUMERIC,
		               (NEW.location ->> 'y')::NUMERIC
		             )
		           )
				, 4326);
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_geom_on_insert_alert ON waze.alerts;
CREATE TRIGGER trig_geom_on_insert_alert
	AFTER INSERT ON waze.alerts
	FOR EACH ROW
	EXECUTE PROCEDURE waze.fn_geom_on_insert_alert();

---- JAMS (line)
CREATE OR REPLACE FUNCTION waze.fn_geom_on_insert_jam() RETURNS TRIGGER AS
$BODY$
BEGIN
	INSERT INTO waze.geoTable(jam_id,geom)
	SELECT NEW.pid,
			ST_SetSRID(
				ST_MakeLine(
		             ST_MakePoint(
		               (NEW.line -> n ->> 'x')::NUMERIC,
		               (NEW.line -> n ->> 'y')::NUMERIC
		             )
		           )
				, 4326)
	FROM generate_series(0, jsonb_array_length(NEW.line)) AS n;
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_geom_on_insert_jam ON waze.jams;
CREATE TRIGGER trig_geom_on_insert_jam
	AFTER INSERT ON waze.jams
	FOR EACH ROW
	EXECUTE PROCEDURE waze.fn_geom_on_insert_jam();

---- IRREGULARITIES (line)
CREATE OR REPLACE FUNCTION waze.fn_geom_on_insert_irreg() RETURNS TRIGGER AS
$BODY$
BEGIN
	INSERT INTO waze.geoTable(irregularity_id,geom)
	SELECT NEW.pid,
			ST_SetSRID(
				ST_MakeLine(
		             ST_MakePoint(
		               (NEW.line -> n ->> 'x')::NUMERIC,
		               (NEW.line -> n ->> 'y')::NUMERIC
		             )
		           )
				, 4326)
	FROM generate_series(0, jsonb_array_length(NEW.line)) AS n;
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_geom_on_insert_irreg ON waze.irregularities;
CREATE TRIGGER trig_geom_on_insert_irreg
	AFTER INSERT ON waze.irregularities
	FOR EACH ROW
	EXECUTE PROCEDURE waze.fn_geom_on_insert_irreg();
