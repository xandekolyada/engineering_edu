#!/bin/bash

DOWNLOAD_URL=$1
CONNECTION_LINK=$2

RESP_CODE="$(curl -w '%{http_code}\n' -L $DOWNLOAD_URL -o events.csv)"
EXIT_STATUS="$?"
if ! [[ $RESP_CODE = 200 ]]; then
  echo "ERROR when fetching data from $1"
  exit 1
elif ! [[ $EXIT_STATUS = 0 ]] ; then
  echo "ERROR when writing data into events.csv"
  exit 1
elif ! [[ "$(file -b --mime-type events.csv)" = "text/csv" ]] ; then
  echo "Downloaded file type is not text/csv"
  exit 1
fi

psql $CONNECTION_LINK -c "
  BEGIN;
  DROP TABLE IF EXISTS events_container;
  CREATE TABLE events_container (                                                                  
    user_id char(15) NOT NULL,
    product_identifier varchar(255) NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    price_in_usd float NOT NULL
  );
  COMMIT;
"

psql $CONNECTION_LINK -c "\\copy events_container FROM 'events.csv' WITH (DELIMITER ',', FORMAT 'csv', ENCODING 'UTF-8', HEADER 'true')"

psql $CONNECTION_LINK -c "
  DO \$\$
  DECLARE total_rows INT := (SELECT COUNT(*) FROM events_container);
  BEGIN
    IF total_rows <> 0 THEN
      DROP TABLE IF EXISTS events;
      CREATE TABLE events (
        id SERIAL PRIMARY KEY,
        user_id char(15) NOT NULL,
        product_identifier varchar(255) NOT NULL,
        start_time timestamp without time zone NOT NULL,
        end_time timestamp without time zone NOT NULL,
        price_in_usd float NOT NULL
      );
      INSERT INTO events(user_id, product_identifier, start_time, end_time, price_in_usd)
      (
        SELECT
          user_id,
          product_identifier,
          start_time,
          end_time,
          price_in_usd
        FROM
          events_container
      );
    END IF;
    DROP TABLE events_container;
  END \$\$;
"

