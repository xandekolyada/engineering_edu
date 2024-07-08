#!/bin/bash

CONNECTION_LINK=$1
CURRENT_DATE_DIR="$(date +"%Y-%m-%d")"

psql $CONNECTION_LINK -c "
  BEGIN;
  CREATE TABLE IF NOT EXISTS stations_raw (
    id integer PRIMARY KEY,
    date date NOT NULL,
    station char(1) NOT NULL,
    msg varchar(12) NOT NULL
  );
  DELETE FROM stations_raw WHERE date = '$CURRENT_DATE_DIR';
  COMMIT;
"

for entry in "$CURRENT_DATE_DIR"/*
do
  psql $CONNECTION_LINK -c "\\copy stations_raw FROM '$entry' WITH (DELIMITER ',', FORMAT 'csv', ENCODING 'UTF-8', HEADER 'true')"
done

psql $CONNECTION_LINK -c "
  BEGIN;
  CREATE TABLE IF NOT EXISTS station_errors AS
  (
    SELECT
      id,
      date,
      station,
      msg,
      CASE
        WHEN total_fails = 1 THEN 'new'
        WHEN total_fails = 2 AND prev_msg IS NULL THEN 'new'
        WHEN total_fails = 2 AND prev_msg IS NOT NULL THEN 'serious'
        WHEN total_fails = 3 THEN 'critical'
      END AS status
    FROM
    (
      SELECT
        id,
        date,
        station,
        msg,
        COUNT(msg IS NOT NULL OR NULL) OVER w AS total_fails,
        LAG(msg, 1) OVER w AS prev_msg
      FROM
      (
        SELECT
          date,
          station,
          MAX(id) AS id,
          'fail' AS msg
        FROM
          stations_raw
        WHERE
          msg = 'fail'
        GROUP BY
          date,
          station
      ) fails
      RIGHT JOIN
      (
        SELECT
          DATE(date) AS date,
          station
        FROM
          GENERATE_SERIES(
            (SELECT MIN(date) - 2 from stations_raw),
            (SELECT MAX(date) from stations_raw),
            '1 DAY'
          ) AS date
        CROSS JOIN
        (
          SELECT DISTINCT
            station
          FROM
            stations_raw
        ) distinct_stations
      ) full_dates
      USING (date, station)
      WINDOW w AS (PARTITION BY station ORDER BY date ASC ROWS 2 PRECEDING)
    ) fails_extended
    WHERE msg IS NOT NULL
  );
  DELETE FROM station_errors WHERE date = '$CURRENT_DATE_DIR';
  INSERT INTO station_errors
  (
    SELECT
      id,
      date,
      station,
      msg,
      CASE
        WHEN total_fails = 1 THEN 'new'
        WHEN total_fails = 2 AND prev_msg IS NULL THEN 'new'
        WHEN total_fails = 2 AND prev_msg IS NOT NULL THEN 'serious'
        WHEN total_fails = 3 THEN 'critical'
      END AS status
    FROM
    (
      SELECT
        id,
        date,
        station,
        msg,
        COUNT(msg IS NOT NULL OR NULL) OVER w AS total_fails,
        LAG(msg, 1) OVER w AS prev_msg
      FROM
      (
        SELECT
          date,
          station,
          MAX(id) AS id,
          'fail' AS msg
        FROM
          stations_raw
        WHERE
          date >= DATE('$CURRENT_DATE_DIR') - 2
          AND msg = 'fail'
        GROUP BY
          date,
          station
      ) fails
      RIGHT JOIN
      (
        SELECT
          DATE(date) AS date,
          station
        FROM
          GENERATE_SERIES(
            DATE('$CURRENT_DATE_DIR') - 2,
            DATE('$CURRENT_DATE_DIR'),
            '1 DAY'
          ) AS date
        CROSS JOIN
        (
          SELECT DISTINCT
            station
          FROM
            stations_raw
        ) distinct_stations
      ) full_dates
      USING (date, station)
      WINDOW w AS (PARTITION BY station ORDER BY date ASC ROWS 2 PRECEDING)
    ) fails_extended
    WHERE
      date = '$CURRENT_DATE_DIR'
      AND msg IS NOT NULL
  );
  COMMIT;
"
