-- PostgeSQL
CREATE TABLE deposits (
  id integer PRIMARY KEY,
  user_id integer NOT NULL,
  amount float NOT NULL,
  created_at timestamp without time zone NOT NULL
);

CREATE TABLE withdrawals (
  id integer PRIMARY KEY,
  user_id integer NOT NULL,
  amount float NOT NULL,
  created_at timestamp without time zone NOT NULL
);

CREATE TABLE date_agg_operations AS
(
  SELECT
    created_at,
    COALESCE(deposits, 0) AS deposits,
    COALESCE(withdrawals, 0) AS withdrawals,
    COALESCE(deposits, 0) - COALESCE(withdrawals, 0) AS revenue
  FROM
  (
    SELECT
      DATE(created_at) AS created_at,
      SUM(amount) AS deposits
    FROM
      deposits
    GROUP BY
      DATE(created_at)
  ) d
  FULL JOIN
  (
    SELECT
      DATE(created_at) AS created_at,
      SUM(amount) AS withdrawals
    FROM
      withdrawals
    GROUP BY
      DATE(created_at)
  ) w
  USING (created_at)
);
ALTER TABLE date_agg_operations ADD PRIMARY KEY (created_at);


CREATE OR REPLACE FUNCTION process_deposits() RETURNS TRIGGER AS $process_deposits$ -- running in READ COMMITTED transaction
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        DATE(created_at) AS created_at,
                        SUM(amount) as deposits
                      FROM
                        old_table
                      GROUP BY
                        DATE(created_at)
                    ) o
                    ON agg.created_at = o.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        deposits = agg.deposits - o.deposits,
                        revenue = agg.revenue - o.deposits;
                    EXIT;
                EXCEPTION
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        ELSIF (TG_OP = 'UPDATE') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        created_at,
                        n.deposits - o.deposits AS deposits
                      FROM
                      (
                        SELECT
                          DATE(created_at) AS created_at,
                          SUM(amount) as deposits
                        FROM
                          new_table
                        GROUP BY
                          DATE(created_at)
                      ) n
                      INNER JOIN
                      (
                        SELECT
                          DATE(created_at) AS created_at,
                          SUM(amount) as deposits
                        FROM
                          old_table
                        GROUP BY
                          DATE(created_at)
                      ) o
                      USING (created_at)
                    ) dif
                    ON agg.created_at = dif.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        deposits = agg.deposits + dif.deposits,
                        revenue = agg.revenue + dif.deposits;
                    EXIT;
                EXCEPTION
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        ELSIF (TG_OP = 'INSERT') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        DATE(created_at) AS created_at,
                        SUM(amount) as deposits
                      FROM
                        new_table
                      GROUP BY
                        DATE(created_at)
                    ) n
                    ON agg.created_at = n.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        deposits = agg.deposits + n.deposits,
                        revenue = agg.revenue + n.deposits
                    WHEN NOT MATCHED THEN
                      INSERT VALUES(n.created_at, n.deposits, 0, n.deposits);
                    EXIT;
                EXCEPTION
                    WHEN UNIQUE_VIOLATION THEN
                    -- do nothing
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$process_deposits$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION process_withdrawals() RETURNS TRIGGER AS $process_withdrawals$ -- running in READ COMMITTED transaction
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        DATE(created_at) AS created_at,
                        SUM(amount) as withdrawals
                      FROM
                        old_table
                      GROUP BY
                        DATE(created_at)
                    ) o
                    ON agg.created_at = o.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        withdrawals = agg.withdrawals - o.withdrawals,
                        revenue = agg.revenue + o.withdrawals;
                    EXIT;
                EXCEPTION
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        ELSIF (TG_OP = 'UPDATE') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        created_at,
                        n.withdrawals - o.withdrawals AS withdrawals
                      FROM
                      (
                        SELECT
                          DATE(created_at) AS created_at,
                          SUM(amount) as withdrawals
                        FROM
                          new_table
                        GROUP BY
                          DATE(created_at)
                      ) n
                      INNER JOIN
                      (
                        SELECT
                          DATE(created_at) AS created_at,
                          SUM(amount) as withdrawals
                        FROM
                          old_table
                        GROUP BY
                          DATE(created_at)
                      ) o
                      USING (created_at)
                    ) dif
                    ON agg.created_at = dif.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        withdrawals = agg.withdrawals + dif.withdrawals,
                        revenue = agg.revenue - dif.withdrawals;
                    EXIT;
                EXCEPTION
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        ELSIF (TG_OP = 'INSERT') THEN
            LOOP
                BEGIN
                    MERGE INTO date_agg_operations agg
                    USING (
                      SELECT
                        DATE(created_at) AS created_at,
                        SUM(amount) as withdrawals
                      FROM
                        new_table
                      GROUP BY
                        DATE(created_at)
                    ) n
                    ON agg.created_at = n.created_at
                    WHEN MATCHED THEN
                      UPDATE SET
                        withdrawals = agg.withdrawals + n.withdrawals,
                        revenue = agg.revenue - n.withdrawals
                    WHEN NOT MATCHED THEN
                      INSERT VALUES(n.created_at, 0, n.withdrawals, -n.withdrawals);
                    EXIT;
                EXCEPTION
                    WHEN UNIQUE_VIOLATION THEN
                    -- do nothing
                    WHEN DEADLOCK_DETECTED THEN
                    -- do nothing
                END;
            END LOOP;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$process_withdrawals$ LANGUAGE plpgsql;

CREATE TRIGGER process_deposits_ins
    AFTER INSERT ON deposits
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_deposits();
CREATE TRIGGER process_deposits_upd
    AFTER UPDATE ON deposits
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_deposits();
CREATE TRIGGER process_deposits_del
    AFTER DELETE ON deposits
    REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_deposits();

CREATE TRIGGER process_withdrawals_ins
    AFTER INSERT ON withdrawals
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_withdrawals();
CREATE TRIGGER process_withdrawals_upd
    AFTER UPDATE ON withdrawals
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_withdrawals();
CREATE TRIGGER process_withdrawals_del
    AFTER DELETE ON withdrawals
    REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT EXECUTE FUNCTION process_withdrawals();
