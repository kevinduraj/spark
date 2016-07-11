-- CREATE KEYSPACE emju WITH replication = {'class': 'NetworkTopologyStrategy', 'bkpoltpvm0': '1'}  ;
CREATE KEYSPACE emju_spark WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}  ;

CREATE TABLE emju_spark.clipped_offer_a_1m (
    retail_customer_id text,
    household_id bigint,
    customer_friendly_program_id text,
    offer_id bigint,
    clip_id text,
    club_card_nbr bigint,
    last_update_user_id text,
    offer_clip_ts timestamp,
    PRIMARY KEY (retail_customer_id, household_id, customer_friendly_program_id, offer_id)
);

CREATE TABLE emju_spark.clipped_offer_b_1m (
    retail_customer_id text,
    household_id bigint,
    customer_friendly_program_id text,
    offer_id bigint,
    clip_id text,
    club_card_nbr bigint,
    last_update_user_id text,
    offer_clip_ts timestamp,
    PRIMARY KEY (retail_customer_id, household_id, customer_friendly_program_id, offer_id)
);

CREATE TABLE emju_spark.clipped_offer_c_1m (
    retail_customer_id text,
    household_id bigint,
    customer_friendly_program_id text,
    offer_id bigint,
    clip_id text,
    club_card_nbr bigint,
    last_update_user_id text,
    offer_clip_ts timestamp,
    PRIMARY KEY (retail_customer_id, household_id, customer_friendly_program_id, offer_id)
);

