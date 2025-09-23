-- Minimal driver: compute for an explicit list of USGS site IDs
-- Usage examples:
--   SELECT firearea.compute_hydro_fire_distances_for_sites('09474000','09471110');
--   -- or with an array:
--   SELECT firearea.compute_hydro_fire_distances_for_sites(ARRAY['09474000','09471110']);

DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_sites(VARIADIC text[]);
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_sites(text[]);

-- Variadic version: allows calling with comma-separated arguments or an array
CREATE OR REPLACE FUNCTION firearea.compute_hydro_fire_distances_for_sites(VARIADIC p_sites text[])
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  s text;
  processed integer := 0;
BEGIN
  -- Guard
  IF p_sites IS NULL OR array_length(p_sites, 1) IS NULL THEN
    RAISE NOTICE 'No sites provided';
    RETURN 0;
  END IF;

  -- Loop distinct sites and invoke per-site compute
  FOR s IN (
    SELECT DISTINCT site FROM unnest(p_sites) AS t(site)
  ) LOOP
    PERFORM firearea.compute_hydro_fire_distances(s);
    processed := processed + 1;
  END LOOP;

  RETURN processed;
END;
$$;

-- Array wrapper (optional): call with an explicit array
CREATE OR REPLACE FUNCTION firearea.compute_hydro_fire_distances_for_sites(p_sites text[])
RETURNS integer
LANGUAGE sql
AS $$
  SELECT firearea.compute_hydro_fire_distances_for_sites(VARIADIC p_sites);
$$;
