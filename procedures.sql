CREATE OR REPLACE PROCEDURE update_dim(
       destination_table text, 
       staging_table text,
       key_column text,
       concomitant_columns text[],
       scdt1_columns text[],
       scdt2_columns text[]
)
LANGUAGE plpgsql
AS $$

DECLARE
    scdt1_set text;
    scdt1_where text;
    scdt2_where text;
    staging_columns text;
    dim_columns text;

BEGIN
--------------------------------------------------------------------------------
                              --VARIABLE DEFINITION--
--------------------------------------------------------------------------------
   select string_agg('s.' || col, ', ')
   into staging_columns
   from unnest(concomitant_columns) as col;

   select string_agg(col, ', ')
   into dim_columns
   from unnest(concomitant_columns) as col;

   select '(' || string_agg('s.' || col || '  IS DISTINCT FROM ' || 'd.' || col, ' OR ') || ')'
   into scdt2_where
   from unnest(scdt2_columns) as col;

   select string_agg(col || ' = ' || 's.' || col, ', ')
   into scdt1_set
   from unnest(scdt1_columns) as col;

   select '(' || string_agg('s.' || col || '  IS DISTINCT FROM ' || 'd.' || col, ' OR ') || ')'
   into scdt1_where
   from unnest(scdt1_columns) as col;

--------------------------------------------------------------------------------
                            --INSERT ONLY NEW RECORDS--
--------------------------------------------------------------------------------
EXECUTE format('
    INSERT INTO %s(%s, valid_from, valid_to, is_current)
    SELECT
        %s,
        CURRENT_DATE,
        ''9999-12-31'',
        TRUE
    FROM %s AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM %s AS d
        WHERE s.%s = d.%s
        );',
    destination_table,
    dim_columns,
    staging_columns,

    staging_table,
    destination_table,

    key_column,
    key_column
);
 
--------------------------------------------------------------------------------
                        -- OVERWRITE CHANGED RECORDS --
--------------------------------------------------------------------------------
EXECUTE format('
    UPDATE %s AS d
    SET %s
    FROM %s AS s
    WHERE
        d.%s = s.%s
        AND d.is_current = TRUE
        AND %s;',
    destination_table,
    scdt1_set,
    staging_table,

    key_column,
    key_column,

    scdt1_where
);

--------------------------------------------------------------------------------
                          -- EXPIRE CHANGED RECORDS --
--------------------------------------------------------------------------------
EXECUTE format('
    UPDATE %s AS d
    SET
        valid_to = CURRENT_DATE,
        is_current = FALSE
    FROM %s AS s
    WHERE
        d.%s = s.%s
        AND d.is_current = TRUE
        AND %s;',
    destination_table,
    staging_table,

    key_column,
    key_column,

    scdt2_where
);

--------------------------------------------------------------------------------
                    -- INSERT NEW VERSION OF EXPIRED RECORDS --
--------------------------------------------------------------------------------
EXECUTE format('
    INSERT INTO %s(%s, valid_from, valid_to, is_current)
    SELECT
        %s,
        CURRENT_DATE,
        ''9999-12-31'',
        TRUE
    FROM %s AS s
    JOIN %s AS d
    ON d.%s = s.%s
    WHERE
        d.is_current = FALSE
        AND d.valid_to = CURRENT_DATE
        AND NOT EXISTS (
            SELECT 1 FROM %s AS d2
            WHERE d2.%s = s.%s AND d2.is_current = TRUE);',
    destination_table,
    dim_columns,
    
    staging_columns,

    staging_table,
    destination_table,

    key_column,
    key_column,

    destination_table,

    key_column,
    key_column
);

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'update_dim failed: %', SQLERRM;
END $$;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

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

