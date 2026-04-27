CREATE TABLE IF NOT EXISTS dim_date(
       date_key            INT             PRIMARY KEY,
       full_date           DATE            NOT NULL,
       day_name            VARCHAR(32)     NOT NULL,
       month_name          VARCHAR(32)     NOT NULL,
       day                 SMALLINT        NOT NULL,
       month               SMALLINT        NOT NULL,
       year                SMALLINT        NOT NULL,
       day_of_week         SMALLINT        NOT NULL,
       day_of_year         SMALLINT        NOT NULL,
       week_of_month       SMALLINT        NOT NULL,
       week_of_year        SMALLINT        NOT NULL,
       quarter_of_year     SMALLINT        NOT NULL,
       is_leap_year        BOOLEAN         NOT NULL,
       is_weekend          BOOLEAN         NOT NULL,
       is_holyday          BOOLEAN         ,
       first_day_of_month  DATE            NOT NULL,
       first_day_of_year   DATE            NOT NULL,
       last_day_of_month   DATE            NOT NULL,
       last_day_of_year    DATE            NOT NULL
);

INSERT INTO dim_date
SELECT
TO_CHAR(dates, 'YYYYMMDD')::INT                                 AS date_key,
dates                                                           AS full_date,
TO_CHAR(dates, 'day')                                           AS day_name,
TO_CHAR(dates, 'month')                                         AS month_name,
TO_CHAR(dates, 'DD')::INT                                       AS day,
TO_CHAR(dates, 'MM')::INT                                       AS month,
TO_CHAR(dates, 'YYYY')::INT                                     AS year,
TO_CHAR(dates, 'D')::INT                                        AS day_of_week,
TO_CHAR(dates, 'DDD')::INT                                      AS day_of_year,
TO_CHAR(dates, 'W')::INT                                        AS week_of_month,
TO_CHAR(dates, 'WW')::INT                                       AS w_year,
TO_CHAR(dates, 'Q')::INT                                        AS quarter_of_year,
EXTRACT(YEAR FROM dates) % 4 = 0
AND (EXTRACT(YEAR FROM dates) % 100 <> 0
    OR EXTRACT(YEAR FROM dates) % 400 = 0)                      AS is_leap_year,
TO_CHAR(dates, 'D')::INT = 1 OR TO_CHAR(dates, 'D')::INT = 7    AS is_weekend,
NULL                                                            AS is_holyday,
DATE_TRUNC('month', dates)::date                                AS first_day_of_month,
DATE_TRUNC('year', dates)::date                                 AS first_day_of_year,
(DATE_TRUNC('month', dates)::date
                     + interval '1 month - 1 day')::DATE        AS last_day_of_month,
(DATE_TRUNC('year', dates)::date
                    + interval '1 year - 1 day')::DATE          AS last_day_of_year

FROM
    (SELECT dates::date
    FROM generate_series(
        now()::date - interval '20 years',
        now()::date + interval '20 years',
        '1 day'::interval) as dates);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);
