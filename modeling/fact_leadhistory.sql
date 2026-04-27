-- fact tables should be partitioned on the date column so they become
-- less computationaly expensive to query

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
     'fact_leadhistory',        -- destination table
     'staging.leadhistory',     -- staging table
     'dim_lead',                -- dimention table(to get sk)
     'id',                      -- pk_fact
     'leadid',                  -- fk_fact
     'id',                      -- pk_dim
     'sk_lead',                 -- sk_dim
     -- columns on both tables(staging table do not have sk)
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
