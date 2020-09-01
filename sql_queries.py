import configparser



# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

stg_immigration_table_drop = "DROP table IF EXISTS stg_immigration_table"
stg_demographics_table_drop = "DROP table IF EXISTS stg_demographics_table"
migration_fact_table_drop = "DROP table IF EXISTS migration_fact_table"
respondent_dim_table_drop = "DROP table IF EXISTS respondent_dim_table"
immi_file_table_drop = "DROP table IF EXISTS immi_file_dim_table"
demographic_table_drop = "DROP table IF EXISTS demographic_dim_table"

# CREATE TABLES
stg_immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_immigration_table (
i94port VARCHAR(256),
i94addr VARCHAR(256),
visapost VARCHAR(256),
entdepa VARCHAR(256),
entdepd VARCHAR(256),
gender VARCHAR(256),
airline VARCHAR(256),
admnum NUMERIC,
fltno VARCHAR(256),
visatype VARCHAR(256),
"month" VARCHAR(256),
"year" VARCHAR(256),
cit VARCHAR(256),
res VARCHAR(256),
visa_code VARCHAR(256),
age_respondent int4,
birth_year VARCHAR(256),
arrival_date VARCHAR(256),
departure_date VARCHAR(256),
arrival_type VARCHAR(256),
date_form_added VARCHAR(256),
admission_date VARCHAR(256),
immi_id int8
);
""")

stg_demographics_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_demographics_table (
race_count int4,
male_population int4,
female_population int4,
total_population int4,
total_veterans int4,
total_foreign_born int4,
state_code VARCHAR(256),
race VARCHAR(256),
avg_household_size NUMERIC,
median_age NUMERIC
);
""")

migration_fact_table_create = ("""
CREATE TABLE IF NOT EXISTS migration_fact_table (
fact_id int4 identity (1,1) sortkey,
immi_id int8 NOT NULL,
admnum NUMERIC NOT NULL,
state_code VARCHAR(256) NOT NULL,
race VARCHAR(256) NOT NULL,
total_population int4,
CONSTRAINT migration_fact_table_pkey PRIMARY KEY (fact_id),
FOREIGN KEY(admnum) REFERENCES respondent_dim_table(admnum),
FOREIGN KEY(immi_id) REFERENCES immi_file_dim_table(immi_id),
FOREIGN KEY(state_code, race) REFERENCES demographic_dim_table(state_code, race)
);
""")

respondent_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS respondent_dim_table (
admnum NUMERIC NOT NULL sortkey,
cit VARCHAR(256),
res VARCHAR(256),
gender VARCHAR(256),
age_respondent int4,
birth_year VARCHAR(256),
airline VARCHAR(256),
fltno VARCHAR(256),
arrival_date VARCHAR(256),
departure_date VARCHAR(256),
CONSTRAINT respondent_dim_table_pkey PRIMARY KEY (admnum)
);
""")

immi_file_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS immi_file_dim_table (
immi_id int8 NOT NULL sortkey,
i94port VARCHAR(256),
i94addr VARCHAR(256),
visapost VARCHAR(256),
entdepa VARCHAR(256),
entdepd VARCHAR(256),
visatype VARCHAR(256),
"month" VARCHAR(256),
"year" VARCHAR(256),
visa_code VARCHAR(256),
arrival_type VARCHAR(256),
date_form_added VARCHAR(256),
admission_date VARCHAR(256),
CONSTRAINT immi_file_dim_table_pkey PRIMARY KEY (immi_id)
);
""")

demographic_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS demographic_dim_table (
state_code VARCHAR(256) NOT NULL sortkey,
race VARCHAR(256) NOT NULL,
race_count int4,
male_population int4,
female_population int4,
total_veterans int4,
total_foreign_born int4,
avg_household_size NUMERIC,
median_age NUMERIC,
CONSTRAINT demographic_dim_table_pkey PRIMARY KEY(state_code, race)
);
""")


# STAGING TABLES

stg_immigration_copy = ("""
COPY stg_immigration_table from {}
CREDENTIALS 'aws_iam_role={}'
REGION 'ap-southeast-2'
IGNOREHEADER 1
DELIMITER ',';
""").format(config.get('S3', 'immigration_data'), config.get('IAM_ROLE', 'arn'))

stg_demographics_copy = ("""
COPY stg_demographics_table FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'ap-southeast-2'
IGNOREHEADER 1
DELIMITER ',';
""").format(config.get('S3', 'demographics_data'), config.get('IAM_ROLE', 'arn'))

# FINAL TABLES

migration_fact_table_insert = ("""
INSERT INTO migration_fact_table (
immi_id,
admnum,
state_code,
race,
total_population
)
SELECT
i.immi_id,
i.admnum,
d.state_code,
d.race,
d.total_population
FROM stg_immigration_table AS i
JOIN stg_demographics_table AS d
ON (i.i94addr = d.state_code);
""")

respondent_dim_table_insert = ("""
INSERT INTO respondent_dim_table (
admnum,
cit,
res,
gender,
age_respondent,
birth_year,
airline,
fltno ,
arrival_date,
departure_date
)
SELECT DISTINCT
i.admnum,
i.cit,
i.res,
i.gender,
i.age_respondent,
i.birth_year,
i.airline,
i.fltno ,
i.arrival_date,
i.departure_date
FROM stg_immigration_table AS i;
""")

immi_file_dim_table_insert = ("""
INSERT INTO immi_file_dim_table (
immi_id,
i94port,
i94addr,
visapost,
entdepa,
entdepd,
visatype,
"month",
"year",
visa_code,
arrival_type,
date_form_added,
admission_date
)
SELECT DISTINCT
i.immi_id,
i.i94port,
i.i94addr,
i.visapost,
i.entdepa,
i.entdepd,
i.visatype,
i."month",
i."year",
i.visa_code,
i.arrival_type,
i.date_form_added,
i.admission_date
FROM stg_immigration_table AS i;
""")

demographic_dim_table_insert = ("""
INSERT INTO demographic_dim_table (
state_code,
race,
race_count,
male_population,
female_population,
total_veterans,
total_foreign_born,
avg_household_size,
median_age
)
SELECT DISTINCT
d.state_code,
d.race,
d.race_count,
d.male_population,
d.female_population,
d.total_veterans,
d.total_foreign_born,
d.avg_household_size,
d.median_age
FROM stg_demographics_table AS d;
""")


# QUERY LISTS
#
create_table_queries = [stg_immigration_table_create, stg_demographics_table_create, respondent_dim_table_create, immi_file_dim_table_create, demographic_dim_table_create,migration_fact_table_create]
drop_table_queries = [stg_immigration_table_drop, stg_demographics_table_drop, migration_fact_table_drop, respondent_dim_table_drop, immi_file_table_drop, demographic_table_drop]
copy_table_queries = [stg_immigration_copy, stg_demographics_copy]
insert_table_queries = [migration_fact_table_insert, respondent_dim_table_insert, immi_file_dim_table_insert, demographic_dim_table_insert]
