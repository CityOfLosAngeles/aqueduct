/* create a trip_geoms table */ 
CREATE TABLE IF NOT EXISTS trips_geoms (
	trip_id UUID NOT NULL,
	points GEOMETRY 
); 
/* process route function */ 
CREATE OR REPLACE FUNCTION process_route(jsonb) 
	RETURNS geometry 
	LANGUAGE 'sql'
	AS $BODY$

	SELECT  
	ST_MakeLine(ARRAY(
        SELECT ST_GeomFromGeoJSON((feature -> 'geometry')::text) as geometry FROM (
            SELECT jsonb_array_elements($1 -> 'features') as feature
        ) features
    ))
$BODY$;
/* trigger function */ 
CREATE OR REPLACE FUNCTION geom_copy() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO
        trips_geoms(trip_id,points)
        VALUES(new.trip_id,process_route(new.route));
           RETURN new;
END;
$BODY$
language plpgsql;
/* trigger itself */ 
CREATE TRIGGER route_process
     AFTER INSERT ON trips
     FOR EACH ROW
     EXECUTE PROCEDURE geom_copy();
