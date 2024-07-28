
Role : Accountadmin

---- Warehouse ----

create warehouse easy_go_wh 
  WAREHOUSE_TYPE = STANDARD 
  WAREHOUSE_SIZE = XSMALL

---Database ------
Create DATABASE EASY_HIST;
Create DATABASE EASY_STAGE;

----Schema-------

create schema easy_hist.bronze;
create schema easy_hist.silver;
create schema easy_hist.gold;

---- stage tables ------------

create table easy_stage.stage_bronze.daily_results (V variant);
create table easy_stage.stage_bronze.daily_schedule (V variant);

------ STORAGE INTEGRATION AND EXTERNAL STAGE-------------

CREATE OR REPLACE STORAGE INTEGRATION easygo_api
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_PROVIDER = 'AZURE'
  AZURE_TENANT_ID = 'c918287d-391f-4bb6-bad9-69040566ba6e' 
  STORAGE_ALLOWED_LOCATIONS = ('*');

create or replace stage stage_bronze.blob_store_stage
STORAGE_INTEGRATION = easygo_api
url = 'azure://satesteasygo.blob.core.windows.net/raw';

---- COPY INTO STAGE -----

truncate table easy_stage.stage_bronze.daily_results;

copy into easy_stage.stage_bronze.daily_results from 'azure://satesteasygo.blob.core.windows.net/raw/cricket_results_2024-07-27_2321.json'
storage_integration = easygo_api
file_format = (
type = 'JSON' 
) 

truncate table easy_stage.stage_bronze.daily_schedule;

copy into easy_stage.stage_bronze.daily_schedule from 'azure://satesteasygo.blob.core.windows.net/raw/cricket_schedule_2024-07-27_2321.json'
storage_integration = easygo_api
file_format = (
type = 'JSON' 
) ;

----- Bronze -------------

create or replace view easy_hist.bronze.daily_results 
as
select 
V:generated_at::date as generated_at,
res.Value:sport_event.id::string as sport_event_id ,
res.Value:sport_event.scheduled::string as scheduled,
res.Value:sport_event.season.end_date::string as season_end_date,
res.Value:sport_event.season.id::string as season_id,
res.Value:sport_event.season.name::string as season_name,
res.Value:sport_event.season.start_date::string as season_start_date,
res.Value:sport_event.season.year::string as season_year,
res.Value:sport_event.start_time_tbd::string as start_time_tbd,
res.Value:sport_event.status::string as status,
res.Value:sport_event.tournament_round.number::string as tournament_round_number,
res.Value:sport_event.tournament_round.type::string as tournament_round_type,
res.Value:sport_event_status.current_inning::string as current_inning,
res.Value:sport_event_status.display_overs::string as display_overs,
res.Value:sport_event_status.display_score::string as display_score,
res.Value:sport_event_status.match_status::string as match_status,
ps.value:away_score::string as away_score,
ps.value:display_score::string as period_scores_display_score,
ps.value:home_score::string as period_scores_home_score,
ps.value:home_wickets::string as period_scores_home_wickets,
ps.value:number::string as period_scores_number,
ps.value:type::string as period_scores_type,
ps.value:away_wickets::string as period_away_wickets,
res.Value:sport_event_status.remaining_overs::string as remaining_overs,
res.Value:sport_event_status.required_run_rate::string as required_run_rate,
res.Value:sport_event_status.status::string as sport_event_status,
res.Value:sport_event_status.target::string as target,
res.Value:sport_event_status.toss_decision::string as toss_decision,
res.Value:sport_event_status.toss_won_by::string as toss_won_by,
res.Value:sport_event_status.winner_id::string as winner_id
from EASY_STAGE.STAGE_BRONZE.DAILY_RESULTS, 
table(flatten(V:results)) res,
table(flatten(res.Value:sport_event_status.period_scores)) ps;

create or replace view easy_hist.bronze.daily_schedule
  as
select 
 V:"generated_at"::date as generated_at,
res.Value:id::string as sport_event_id ,
res.Value:scheduled::string as scheduled,
res.Value:season.end_date::string as season_end_date,
res.Value:season.id::string as season_id,
res.Value:season.name::string as season_name,
res.Value:season.start_date::string as season_start_date,
res.Value:season.year::string as season_year,
res.Value:start_time_tbd::string as start_time_tbd,
res.Value:status::string as status,
res.Value:tournament_round.number::string as tournament_round_number,
res.Value:tournament_round.type::string as tournament_round_type,
res.Value:tournament.id::varchar as id,
res.Value:tournament.gender::varchar as gender,
res.Value:tournament.name::varchar as name,
res.Value:tournament.type::varchar as type,
res.Value:tournament.sport.id::varchar as sport_id,
res.Value:tournament.sport.name::varchar as sport_name,
res.Value:tournament.category.id::varchar as category_id,
res.Value:tournament.category.name::varchar as category_name,
res.Value:tournament.category.country_code::varchar as category_country_code,
res.Value:venue.id::varchar as venue_id,
res.Value:venue.name::varchar as venue_name,
res.Value:venue.capacity::varchar as venue_capacity,
res.Value:venue.city_name::varchar as venue_city_name,
res.Value:venue.country_name::varchar as venue_country_name,
res.Value:venue.map_coordinates::varchar as venue_map_coordinates,
res.Value:venue.country_code::varchar as venue_country_code,
res.Value:venue.timezone::varchar as venue_timezone,
c.Value:id::varchar as competitor_id,
c.Value:name::varchar as competitor_name,
c.Value:country::varchar as competitor_country,
c.Value:country_code::varchar as competitor_country_code,
c.Value:abbreviation::varchar as competitor_abbreviation,
c.Value:gender::varchar as competitor_gender,
c.Value:qualifier::varchar as competitor_qualifier
from EASY_STAGE.STAGE_BRONZE.DAILY_SCHEDULE, 
table(flatten(V:sport_events)) res,
table(flatten(res.Value:competitors)) c;


---- season dimension -----

--Silver
create or replace view easy_hist.silver.season_details
as
select season_id, season_name, season_start_date, season_end_date, season_year, generated_at 
from easy_hist.bronze.daily_schedule s
group by season_id, season_name, season_start_date, season_end_date, season_year, generated_at;
--Gold
create or replace table easy_hist.gold.dim_season
as
select * from easy_hist.silver.season_details;

--- competitor dimension -----

--Silver
create or replace view easy_hist.silver.team_details
as
select competitor_id, competitor_name, competitor_country, competitor_gender, competitor_country_code, competitor_abbreviation, generated_at
from easy_hist.bronze.daily_schedule 
group by competitor_id, competitor_name, competitor_country, competitor_gender, competitor_country_code, competitor_abbreviation, generated_at;

--Gold
create or replace table easy_hist.gold.dim_teams
as
select * from easy_hist.silver.team_details;

-- results ----

select sport_event_id, season_id, season_name, toss_won_by, winner_id
from easy_hist.bronze.daily_results
group by sport_event_id, season_id, season_name, toss_won_by, winner_id;

--- daily match winner details ----

--Silver
create or replace view easy_hist.silver.match_result
as
select r.*, c.competitor_name as winner_name from 
(select sport_event_id, season_id, season_name, toss_won_by, winner_id
from easy_hist.bronze.daily_results
group by sport_event_id, season_id, season_name, toss_won_by, winner_id) r
left join easy_hist.silver.team_details c
on r.winner_id = c.competitor_id;

--Gold
create or replace table easy_hist.gold.dim_match_result
as
select * from easy_hist.silver.match_result;