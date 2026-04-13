-- creates a fact table from the dim_lead dimenssion table

CREATE OR REPLACE PROCEDURE update_fact(
       destination_table    TEXT,
       staging_fact_table   TEXT,
       dim_table            TEXT,
       pk_fact              TEXT,
       fk_fact              TEXT,
       pk_dim               TEXT,
       sk_dim               TEXT,
       concomitant_columns  TEXT[]
)

LANGUAGE plpgsql
AS $$

DECLARE
    staging_columns TEXT;

BEGIN
--------------------------------------------------------------------------------
                              --VARIABLE DEFINITION--
--------------------------------------------------------------------------------
SELECT string_agg('s.' || col, ', ')
INTO staging_columns
FROM UNNEST(concomitant_columns) AS col;

--------------------------------------------------------------------------------
                            --INSERT ONLY NEW RECORDS--
--------------------------------------------------------------------------------
EXECUTE format('
    WITH staging_with_sk AS(
        SELECT
            d.%s,
            %s
        FROM %s AS s
        JOIN %s AS d
        ON s.%s = d.%s AND d.is_current = TRUE 
    )
    INSERT INTO %s(%s, %s)
    SELECT %s, %s
    FROM staging_with_sk AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM %s AS f                  
        WHERE f.%s = s.%s);',
    sk_dim,
    staging_columns,
    staging_fact_table,
    dim_table,
    fk_fact,
    pk_dim,

    destination_table,
    sk_dim,
    array_to_string(concomitant_columns, ', '),
    sk_dim,
    staging_columns,
    destination_table,
    pk_fact,
    pk_fact        
);

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'update_fact failed: %', SQLERRM;
END $$;


CREATE TABLE IF NOT EXISTS fact_leadhistory(
       sk_lead             INT         NOT NULL REFERENCES dim_lead(sk_lead),
       createdbyid         VARCHAR     NOT NULL,
       createddate         VARCHAR     NOT NULL,
       field               VARCHAR,
       id                  VARCHAR     PRIMARY KEY,
       isdeleted           VARCHAR,
       leadid              VARCHAR     NOT NULL,
       newvalue            VARCHAR,
       oldvalue            VARCHAR
);

CALL update_fact(
     'fact_leadhistory',
     'staging.leadhistory',
     'dim_lead',
     'id',
     'leadid',
     'id',
     'sk_lead',
     array['createdbyid', 'createddate', 'field', 'id', 'isdeleted', 'leadid', 'newvalue', 'oldvalue']
);

-- createdbyid
-- createddate
-- field
-- id
-- isdeleted
-- leadid
-- newvalue
-- oldvalue
