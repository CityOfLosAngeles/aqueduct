-- Indexes for the MDS trips and status changes tables.

ALTER TABLE public.trips_geoms
    DROP CONSTRAINT IF EXISTS pk_trip_geom_id;
ALTER TABLE public.trips_geoms
    ADD CONSTRAINT pk_trip_geom_id PRIMARY KEY (trip_id);

CREATE INDEX IF NOT EXISTS idx_status_changes_event_time
    ON public.status_changes USING btree
    (event_time ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_trips_start_time
    ON public.trips USING btree
    (start_time ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_trips_end_time
    ON public.trips USING btree
    (end_time ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_trip_geom_points
    ON public.trips_geoms USING gist
    (points)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_status_change_geoms_event_location_geom
    ON public.status_change_geoms USING gist
    (event_location_geom)
    TABLESPACE pg_default;
