CREATE OR REPLACE TABLE geolab.geography.nl_lte_with_coverage AS
SELECT geom,
       cell_range,
       carto.carto.st_buffer(geom, least(cell_range, 2000), 5) AS coverage
FROM geolab.geography.nl_lte
ORDER BY st_geohash(geom);

ALTER TABLE geolab.geography.nl_lte_with_coverage ADD SEARCH OPTIMIZATION ON GEO(geom); 

// RUN IN CARTO
SELECT c.coverage AS geom
FROM geolab.geography.nl_lte_with_coverage c
JOIN geolab.geography.nl_administrative_areas b 
  ON st_intersects(b.geom, c.geom)
WHERE TYPE = 'Municipality'
  AND municipality_name = 'Angerlo';


CREATE OR REPLACE FUNCTION geolab.geography.py_union_agg(g1 array)
RETURNS GEOGRAPHY
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = ('shapely')
HANDLER = 'udf'
AS $$
from shapely.ops import unary_union
from shapely.geometry import shape, mapping
def udf(g1):
    shape_union = unary_union([shape(i) for i in g1])
    shape_union = shape_union.simplify(0.000001)
    return mapping(shape_union)
$$;

// RUN IN CARTO
SELECT geolab.geography.py_union_agg(array_agg(st_asgeojson(c.coverage))) AS geom
FROM geolab.geography.nl_lte_with_coverage c
JOIN geolab.geography.nl_administrative_areas b 
  ON st_intersects(b.geom, c.geom)
WHERE type = 'Municipality'
AND municipality_name = 'Angerlo';


CREATE OR REPLACE TABLE geolab.geography.nl_municipalities_coverage AS
SELECT municipality_name,
       any_value(geom) AS municipality_geom,
       st_intersection(municipality_geom, 
                       geolab.geography.py_union_agg(array_agg(st_asgeojson(coverage)))) AS coverage_geom,
       round(st_area(coverage_geom)/st_area(municipality_geom), 2) AS coverage_ratio
FROM
  (SELECT c.coverage AS coverage,
          b.municipality_name AS municipality_name,
          b.geom
   FROM geolab.geography.nl_lte_with_coverage c
   INNER JOIN geolab.geography.nl_administrative_areas b 
      ON st_intersects(b.geom, c.coverage)
   WHERE TYPE = 'Municipality')
GROUP BY municipality_name
ORDER BY st_geohash(municipality_geom);

ALTER TABLE geolab.geography.nl_municipalities_coverage ADD SEARCH OPTIMIZATION ON GEO(municipality_geom);

// RUN IN CARTO
SELECT coverage_geom AS geom,
       coverage_ratio
FROM geolab.geography.nl_municipalities_coverage;

// RUN IN CARTO
SELECT sum(st_length(st_intersection(coverage.coverage_geom, roads.geo_cordinates))) AS covered_length,
       sum(st_length(st_intersection(coverage.municipality_geom, roads.geo_cordinates))) AS total_length,
       round(100 * covered_length / total_length, 2) AS "Coverage, %"
FROM osm_nl.netherlands.v_road roads,
     geolab.geography.nl_municipalities_coverage coverage
WHERE st_intersects(coverage.municipality_geom, roads.geo_cordinates)
  AND roads.class in ('primary', 'motorway');


CREATE OR REPLACE TABLE geolab.geography.nl_lte_coverage_h3 AS
// First estimate compute H3 cells and estimate number of H3 cells within range
WITH nl_lte_h3 AS (
    SELECT row_number() OVER(ORDER BY NULL) AS id,
           cell_range,
           H3_POINT_TO_CELL(geom, 9)::int AS h3,
           round(least(cell_range, 2000) / 400)::int AS h3_cell_range
    FROM geolab.geography.nl_lte
),
// Find all neighboring cells and calculate signal strength in them
h3_neighbors AS (
  SELECT id,
         p.value::int AS h3,
         H3_GRID_DISTANCE(n.h3, p.value::int) AS H3_GRID_DISTANCE,
         // decay model for signal strength:
         100 * pow(1 - H3_GRID_DISTANCE / (h3_cell_range + 1), 2) AS signal_strength
  FROM nl_lte_h3 n,
       table(flatten(INPUT => H3_GRID_DISK(n.h3, h3_cell_range))) p)
SELECT h3, 
       // maximum signal strength with noise:
       max(signal_strength) * uniform(0.8, 1::float, random()) AS signal_strength
FROM h3_neighbors
GROUP BY h3
ORDER BY h3;

// RUN IN CARTO
SELECT h3,
       signal_strength
FROM geolab.geography.nl_lte_coverage_h3;



CREATE OR REPLACE table geolab.geography.nl_roads_h3 AS 
// import roads from OSM:
WITH roads AS (
  SELECT row_number() over(ORDER BY NULL) AS road_id,
        geo_cordinates AS geom
  FROM OSM_NL.NETHERLANDS.V_ROAD roads
  WHERE class IN ('primary', 'motorway')
    AND st_dimension(geo_cordinates) = 1
),
// In order to compute H3 cells corresponding to each road you need to first
// split roads into the line segments. You do it using the ST_POINTN function
segments AS (
  SELECT road_id,
          value::integer AS segment_id,
          st_makeline(st_pointn(geom, segment_id), st_pointn(geom, segment_id + 1)) AS SEGMENT,
          geom,
          H3_POINT_TO_CELL(st_centroid(SEGMENT), 9)::int AS h3_center
  FROM roads,
       LATERAL flatten(array_generate_range(1, st_npoints(geom)))) 
// Next table build the H3 cells covering the roads
// For each line segment you find a corresponding H3 cell and then aggregate by road id and H3
// At this point you switched from segments to H3 cells covering the roads.
SELECT road_id,
       h3_center AS h3,
       any_value(geom) AS road_geometry
FROM segments
GROUP BY 1, 2
ORDER BY h3;


CREATE OR REPLACE TABLE geolab.geography.osm_nl_not_covered AS
SELECT road_id,
       any_value(road_geometry) AS geom,
       avg(ifnull(signal_strength, 0.0)) AS avg_signal_strength,
       iff(avg_signal_strength >= 50, 'OK Signal', 'No Signal') AS signal_category
FROM geolab.geography.nl_roads_h3 roads_h3
LEFT JOIN geolab.geography.nl_lte_coverage_h3 cells ON roads_h3.h3 = cells.h3
GROUP BY road_id
ORDER BY st_geohash(geom);

ALTER TABLE geolab.geography.osm_nl_not_covered ADD SEARCH OPTIMIZATION ON GEO(geom);

// RUN IN CARTO
SELECT signal_category,
       SUM(ST_LENGTH(geom)/1000)::int AS total_km
FROM geolab.geography.osm_nl_not_covered
GROUP BY signal_category;