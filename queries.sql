-- Query 1: Busiest stations
/* Clean the raw data by filtering out unreallistically short or long rides.
   Extract dates and times into new columns. */
WITH cleaned_trips AS(
  SELECT
   start_station_name,
   EXTRACT(DAYOFWEEK FROM start_time) AS day_of_week,
   EXTRACT(HOUR FROM start_time) AS hour_of_day,
   duration_minutes
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE duration_minutes BETWEEN 1 AND 180
),

-- Add a new column day_type that categorizes each trip as either a Weekday or Weekend based on day_of_week.
classified_days AS(
  SELECT
    *,
    CASE
     WHEN day_of_week BETWEEN 2 AND 6 THEN 'Weekday'
     ELSE 'Weekend'
    END AS day_type
  FROM cleaned_trips
),

/* Group rides by station, day type, and hour.
   Count how many rides started at each station during a specific hour and day type. */
hourly_rides AS (
  SELECT
    start_station_name,
    day_type,
    hour_of_day,
    COUNT(*) AS ride_count
  FROM classified_days
  GROUP BY start_station_name, day_type, hour_of_day
),

/* Rank each hour for each station and day type by how many rides occured.
   Enable the subsequent section to keep only the top rank for each pair. */
peak_hour_ranked AS(
  SELECT
    start_station_name,
    day_type,
    hour_of_day AS peak_hour,
    ride_count,
    ROW_NUMBER() OVER(
      PARTITION BY start_station_name, day_type
      ORDER BY ride_count DESC
    ) AS rank
  FROM hourly_rides
)

/* Assign rank 1 to the busiest hour at each station, separately for weekdays and weekends.
   Return the busiest hour for each station on weekdays and weekends.
   Limit output to the top 5 busiest combinations. */
SELECT
start_station_name,
day_type,
peak_hour,
ride_count
FROM peak_hour_ranked
WHERE rank = 1
ORDER BY ride_count DESC
LIMIT 5;

--------------------------------------------------------

-- Query 2: Year-over-year changes
-- Calculate total rides per station per year.
WITH yearly_ridership AS(
  SELECT
    start_station_name,
    EXTRACT(YEAR FROM start_time) AS year,
    COUNT(*) AS total_rides
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
GROUP BY start_station_name, year
),

-- Calculate year-over-year growth and percentage change per station.
yearly_ridership_with_growth AS(
  SELECT
  start_station_name,
  year,
  total_rides,

  -- Absolute growth in rides compared to previous year.
  total_rides - LAG(total_rides) OVER (
    PARTITION BY start_station_name ORDER BY year
  ) AS YoY_growth,
  -- Percentage growth, using SAFE_DIVIDE to avoid dividing by zero.
  SAFE_DIVIDE(
    total_rides - LAG(total_rides) OVER (
    PARTITION BY start_station_name ORDER BY year
  ),
  LAG(total_rides) OVER (
      PARTITION BY start_station_name ORDER BY year
    )
  ) * 100 AS percentage_change
FROM yearly_ridership
),

-- Identify station-year pairs with the largest growth or in ridership.
top_5_growth AS (
  SELECT
    start_station_name,
    year,
    total_rides,
    YoY_growth,
    ROUND(percentage_change, 2) AS percentage_change
  FROM yearly_ridership_with_growth
  WHERE YoY_growth IS NOT NULL
  ORDER BY percentage_change DESC
  LIMIT 5
),

-- Identify station-year pairs with the largest growth or in ridership.
bottom_5_decline AS (
    SELECT
    start_station_name,
    year,
    total_rides,
    YoY_growth,
    ROUND(percentage_change, 2) AS percentage_change
  FROM yearly_ridership_with_growth
  WHERE YoY_growth IS NOT NULL
  ORDER BY percentage_change ASC
  LIMIT 5
)

-- Combine both top 5 and bottom 5 into one set of results.
SELECT *
FROM top_5_growth
UNION ALL
SELECT *
FROM bottom_5_decline;
