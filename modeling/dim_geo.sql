CREATE TABLE IF NOT EXISTS dim_geo(
    sk_geo                             INT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    municipio_id                       VARCHAR NOT NULL, 
    municipio_nome                     VARCHAR,
    microrregiao_id                    VARCHAR,
    microrregiao_nome                  VARCHAR,
    mesorregiao_id                     VARCHAR,
    mesorregiao_nome                   VARCHAR,
    uf_id                              VARCHAR,
    uf_nome                            VARCHAR,
    uf_sigla                           VARCHAR,
    regiao_id                          VARCHAR,
    regiao_nome                        VARCHAR,
    regiao_sigla                       VARCHAR,
    regiao_imediata_id                 VARCHAR,
    regiao_imediata_nome               VARCHAR,
    regiao_intermediaria_id            VARCHAR,
    regiao_intermediaria_nome          VARCHAR,
    regiao_intermediaria_uf_id         VARCHAR,
    regiao_intermediaria_uf_nome       VARCHAR,
    regiao_intermediaria_uf_sigla      VARCHAR,
    regiao_intermediaria_regiao_id     VARCHAR,
    regiao_intermediaria_regiao_nome   VARCHAR,
    regiao_intermediaria_regiao_sigla  VARCHAR,
    valid_from                         DATE    NOT NULL,
    valid_to                           DATE    DEFAULT '9999-12-31' NOT NULL,
    is_current                         BOOLEAN NOT NULL DEFAULT TRUE
);

CALL update_dim(
     'dim_geo',
     'staging.ibge_municipios',
     'municipio_id',

     array['municipio_id', 'municipio_nome', 'microrregiao_id', 'microrregiao_nome', 'mesorregiao_id', 'mesorregiao_nome', 'uf_id', 'uf_nome', 'uf_sigla', 'regiao_id', 'regiao_nome', 'regiao_sigla', 'regiao_imediata_id', 'regiao_imediata_nome', 'regiao_intermediaria_id', 'regiao_intermediaria_nome', 'regiao_intermediaria_uf_id', 'regiao_intermediaria_uf_nome', 'regiao_intermediaria_uf_sigla', 'regiao_intermediaria_regiao_id', 'regiao_intermediaria_regiao_nome', 'regiao_intermediaria_regiao_sigla'],

     array['municipio_id', 'microrregiao_id', 'mesorregiao_id', 'uf_id', 'regiao_id', 'regiao_imediata_id', 'regiao_intermediaria_id', 'regiao_intermediaria_uf_id', 'regiao_intermediaria_regiao_id'],

     array['municipio_nome', 'microrregiao_nome', 'mesorregiao_nome']
);
