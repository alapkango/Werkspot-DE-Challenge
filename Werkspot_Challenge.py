#import libraries
import pandas as pd
import numpy as np
import sqlalchemy
import re

#read the csv file
event_log = pd.read_csv("event_log.csv",delimiter=';')


#creating event_type_dim dataframe
event_type_dim_tmp_1= event_log['event_type'].drop_duplicates()
event_type_dim=event_type_dim_tmp_1.to_frame()
event_type_dim = event_type_dim.reset_index(drop=True)
#assigning event_type_id
event_type_dim = event_type_dim.rename_axis('event_type_id').reset_index()
event_type_dim['event_type_id']=event_type_dim['event_type_id']+1
event_type_dim[['event_type_id']] = event_type_dim[['event_type_id']].apply(pd.to_numeric) 
event_type_dim['refresh_date']=pd.datetime.now().date()


#creating service_type_dim dataframe
service_type_dim_tmp_1=event_log['meta_data'].drop_duplicates() 
service_type_dim_tmp_1=service_type_dim_tmp_1.to_frame()
#splitting the metadata column
service_type_dim_tmp_1[['service_id','service_type_name_nl','service_type_name_en','lead_fee']] = service_type_dim_tmp_1.meta_data.str.split("_",expand=True,)
service_type_dim_tmp_1=service_type_dim_tmp_1.drop(columns=['lead_fee','meta_data'])
service_type_dim_tmp_1=service_type_dim_tmp_1.drop_duplicates().fillna('-1')
service_type_dim_tmp_1=service_type_dim_tmp_1.dropna(axis=0, how='all')
service_type_dim=service_type_dim_tmp_1
service_type_dim[['service_id']] = service_type_dim[['service_id']].apply(pd.to_numeric) 
service_type_dim['refresh_date']=pd.datetime.now().date()


#creating event_fact dataframe
event_fact_tmp_1=event_log
event_fact_tmp_1[['service_id','service_type_name_nl','service_type_name_en','lead_fee']] = event_fact_tmp_1.meta_data.str.split("_",expand=True,)
# create a list of our conditions
conditions = [
    (event_fact_tmp_1['event_type'] == 'created_account'),
    (event_fact_tmp_1['event_type'] == 'became_able_to_propose') ,
   (event_fact_tmp_1['event_type'] == 'became_unable_to_propose') ,
    (event_fact_tmp_1['event_type'] == 'proposed')
    ]
values = [1, 2, 3, 4]
event_fact_tmp_1['event_type_id'] = np.select(conditions, values)
event_fact_tmp_1=event_fact_tmp_1.drop(columns=['event_type','meta_data','service_type_name_nl','service_type_name_en'])
event_fact_tmp_1['service_id']=event_fact_tmp_1['service_id'].fillna('-1')
event_fact_tmp_1['lead_fee']=event_fact_tmp_1['lead_fee'].fillna('0')
event_fact_tmp_1[['service_id','lead_fee']] = event_fact_tmp_1[['service_id','lead_fee']].apply(pd.to_numeric) 
event_fact_tmp_1[['created_at']] = event_fact_tmp_1[['created_at']].apply(pd.to_datetime) 
event_fact_tmp_1 = event_fact_tmp_1.rename(columns = {'professional_id_anonymized': 'professional_id', 'created_at': 'event_creation_dt'}, inplace = False)
event_fact=event_fact_tmp_1
event_fact['refresh_date']=pd.datetime.now().date()

#creating tables in postgres
from sqlalchemy import create_engine, MetaData
import psycopg2

#reading config file

from jproperties import Properties
configs = Properties()
with open('config.properties', 'rb') as config_file:
    configs.load(config_file)
	
db_nm=str({configs.get("DB_NM").data})
db_nm=re.sub('[^A-Za-z0-9]+','',db_nm)

db_host=str({configs.get("DB_HOST").data})
db_host=re.sub('[^A-Za-z0-9]+','',db_host)

db_schema=str({configs.get("DB_SCHEMA").data})
db_schema=re.sub('[^A-Za-z0-9]+','',db_schema)


db_user=str({configs.get("DB_User").data})
db_user=re.sub('[^A-Za-z0-9]+','',db_user)

db_pwd=str({configs.get("DB_PWD").data})
db_pwd=re.sub('[^A-Za-z0-9]+','',db_pwd)

db_port=str({configs.get("DB_PORT").data})
db_port=re.sub('[^A-Za-z0-9]+','',db_port)

conn = psycopg2.connect(
   database=db_nm, user=db_user, password=db_pwd, host=db_host, port= db_port
)

#schema creation

conn.autocommit = True
cursor = conn.cursor()
cursor.execute('CREATE SCHEMA IF NOT EXISTS "WERKSPOT_DB"')

engine = create_engine('postgresql://'+db_user+':'+db_pwd+'@'+db_host+'/'+db_nm)
meta = sqlalchemy.MetaData(engine, schema='WERKSPOT_DB')
meta.reflect(engine, schema='WERKSPOT_DB')
pdsql = pd.io.sql.SQLDatabase (engine, meta=meta)

# table creation
pdsql.to_sql(event_type_dim, 'EVENT_TYPE_DIM',if_exists='replace',index=False)
pdsql.to_sql(service_type_dim, 'SERVICE_TYPE_DIM',if_exists='replace',index=False)
pdsql.to_sql(event_fact, 'EVENT_FACT',if_exists='replace',index=False)

# table creation
conn.autocommit = True
cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS  "WERKSPOT_DB"."AVAILABILITY_SNAPSHOT" ( snapshot_date date,active_professionals_count bigint,refresh_date date);')


cursor.execute('DELETE FROM "WERKSPOT_DB"."AVAILABILITY_SNAPSHOT" ;')

cursor.execute('''INSERT INTO "WERKSPOT_DB"."AVAILABILITY_SNAPSHOT"  
	select snapshot_date,active_professionals_count,refresh_date from 
(select snapshot_date,active_professionals_count,refresh_date,
row_number() over (partition by snapshot_date order by active_professionals_count desc) rnk2
from
(select * from
(select cast(event_creation_dt as date) as snapshot_date,count(distinct professional_id) as active_professionals_count,current_date as refresh_date
	from
	(
	select a.event_creation_dt,a.professional_id,a.event_type_id,b.event_type,
	row_number() over (partition by a.professional_id,cast(a.event_creation_dt as date) order by a.event_creation_dt desc)
	rnk
	from 
	"WERKSPOT_DB"."EVENT_FACT"  a inner join "WERKSPOT_DB"."EVENT_TYPE_DIM" b
	on a.event_type_id=b.event_type_id and b.event_type not in ('created_account','proposed')
	and cast(a.event_creation_dt as date)<='2020-03-10'
	)ref where rnk=1 and event_type='became_able_to_propose'
	group by cast(event_creation_dt as date),current_date order by  cast(event_creation_dt as date)
 )ref
union
select  distinct cast( a.event_creation_dt as date )  as snapshot_date,0 as active_professionals_count,current_date as refresh_date
from 
	"WERKSPOT_DB"."EVENT_FACT"  a where cast(a.event_creation_dt as date)<='2020-03-10'
) ref1
 )ref2 where rnk2=1 order by snapshot_date asc''')