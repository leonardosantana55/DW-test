-- the goal of this script is to create a dimentional table of mixed scd types 1 and 2.

CREATE OR REPLACE PROCEDURE update_dim(
       destination_table text, 
       origin_table text,
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

    origin_table,
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
    origin_table,

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
    origin_table,

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

    origin_table,
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

CREATE TABLE IF NOT EXISTS dim_lead(
       sk_lead      INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       id           VARCHAR         NOT NULL,
       name         VARCHAR,
       city         VARCHAR,
       status       VARCHAR,
       valid_from   DATE            NOT NULL,
       valid_to     DATE            DEFAULT '9999-12-31' NOT NULL,
       is_current   BOOLEAN         NOT NULL DEFAULT TRUE
);

CALL update_dim(
   'dim_lead',
   'staging.lead',
   'id',
   array['id', 'name', 'city', 'status'],
   array['name'],
   array['city', 'status']
);

-- steps
-- 1. insert only new values(new system keys)
-- 2. update records for columns for scd t1
-- 3. update(expire) records for columns for scd t2
-- 4. insert records for expired records of scd t2 columns

 -- id,
 -- isdeleted,
 -- masterrecordid,
 -- lastname,
 -- firstname,
 -- salutation,
 -- middlename,
 -- name,
 -- recordtypeid,
 -- title,
 -- company,
 -- street,
 -- city,
 -- state,
 -- postalcode,
 -- country,
 -- statecode,
 -- countrycode,
 -- latitude,
 -- longitude,
 -- geocodeaccuracy,
 -- address,
 -- phone,
 -- mobilephone,
 -- email,
 -- website,
 -- photourl,
 -- leadsource,
 -- status,
 -- industry,
 -- rating,
 -- currencyisocode,
 -- numberofemployees,
 -- ownerid,
 -- isconverted,
 -- converteddate,
 -- convertedaccountid,
 -- convertedcontactid,
 -- convertedopportunityid,
 -- isunreadbyowner,
 -- createddate,
 -- createdbyid,
 -- lastmodifieddate,
 -- lastmodifiedbyid,
 -- systemmodstamp,
 -- lastactivitydate,
 -- lastvieweddate,
 -- lastreferenceddate,
 -- partneraccountid,
 -- jigsaw,
 -- jigsawcontactid,
 -- emailbouncedreason,
 -- emailbounceddate,
 -- actualcontractenddate__c,
 -- actualsupplier__c,
 -- branchline__c,
 -- cceeagent__c,
 -- cnaelookup__c,
 -- cnaesector__c,
 -- cpfcnpj__c,
 -- category__c,
 -- city__c,
 -- complement__c,
 -- currentserviceprovider__c,
 -- demand__c,
 -- department__c,
 -- district__c,
 -- electricpowerbill__c,
 -- number__c,
 -- outsourcedmaintenance__c,
 -- serasaconsultationdate__c,
 -- stateinscription__c,
 -- state__c,
 -- street__c,
 -- submarket__c,
 -- tradingname__c,
 -- voltageclass__c,
 -- zipcode__c,
 -- buildedarea__c,
 -- businessline__c,
 -- campaignutm__c,
 -- lossdescription__c,
 -- corporatename__c,
 -- consumerclass__c,
 -- contractduration__c,
 -- contractstartdate__c,
 -- supplier__c,
 -- contractenddate__c,
 -- averageenergyconsumption__c,
 -- contactcpf__c,
 -- energyplacetermsandconditionsdate__c,
 -- metier__c,
 -- mktcontactid__c,
 -- leadenergysource__c,
 -- ismanagementcompany__c,
 -- role__c,
 -- saleprice__c,
 -- convertedconsumerunits__c,
 -- observation__c,
 -- websiteoption__c,
 -- winnersimulationselected__c,
 -- mediachannel__c,
 -- calculatesladefault__c,
 -- dayssincelastevolution__c,
 -- disqualificationreason__c,
 -- externalurl__c,
 -- salepricemarket__c,
 -- googleclickid__c,
 -- form_name__c,
 -- http_referrer__c,
 -- utm_content__c,
 -- utm_medium__c,
 -- utm_source__c,
 -- engiecompany__c,
 -- salesaccountteam__c,
 -- lead_full_id__c,
 -- leadcaptor__c,
 -- distributor__c,
 -- createdbyopco__c,
 -- ebeenergysource__c,
 -- numberofconsumerunits__c,
 -- operationtype__c,
 -- responsibledepartament__c,
 -- supplyenddate__c,
 -- supplystartdate__c,
 -- companysize__c,
 -- quotelimitsenddate__c,
 -- salesteam__c,
 -- foundationdate__c,
 -- originalseller__c,
 -- volume__c,
 -- energyconsumemonth__c,
 -- manager__c,
 -- anticipationpossibility__c,
 -- auctiondeliverydate__c,
 -- auctionpercentsold__c,
 -- auctionsale__c,
 -- balance__c,
 -- convertedmanageraccount__c,
 -- convertedmanagercontact__c,
 -- deliverysubmarket__c,
 -- generationsource__c,
 -- interestedproducts__c,
 -- managercpfcnpj__c,
 -- managercategory__c,
 -- managercontactname__c,
 -- manageremail__c,
 -- managername__c,
 -- managerphone__c,
 -- networthdate__c,
 -- networth__c,
 -- othergenerationsources__c,
 -- structuredoptions__c,
 -- technicalclassificationdate__c,
 -- technicalclassification__c,
 -- totalwarranty__c,
 -- uncontractedwarranty__c,
 -- additionalcontact__c,
 -- requestedquoteexpirydate__c,
 -- cnpjconsulted__c,
 -- competition__c,
 -- generator__c,
 -- highvoltage__c,
 -- lossreason__c,
 -- negotiateddiscountmax__c,
 -- negotiateddiscountmin__c,
 -- neoway__c,
 -- sazonalidade__c,
 -- secondary_lost_reason__c,
 -- branches__c
