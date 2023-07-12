from pprint import pprint
import datetime
import json
import psycopg2
import dateutil.parser
from   psycopg2.extras import register_default_jsonb

register_default_jsonb(conn_or_curs = None, globally = True, loads = None)

create_sql = \
"""
create table data (
id serial primary key,
dt timestamp not null,
track_id text,
track_name text,
track_type integer, 
delta real, 
gps jsonb,
gsm jsonb, 
inet jsonb,
ytube jsonb,
instg jsonb,
level integer,
device_model text,
device_id text
);
"""

def add_data_from_logs():

    with open('./logs/messages.log', 'r') as f: data = f.readlines()
    
    con = ''
    cur = ''
    try:
        con = psycopg2.connect(host = '127.0.0.1', port = '5432', user = 'cyber', password = 'sheep', database = 'cybermeter')
        cur = con.cursor()
        for row in data:
            x = row.split(']: ')[1]
            y = json.loads(x.replace("'", '"'))
            #y['track_id'] = 'dsfsdfsfsfd'
            sql = """
            insert into data (dt, track_id, track_name, track_type, delta, gps, gsm, 
                              inet, ytube, instg, level, device_model, device_id) 
                              values (%(dt)s, %(track_id)s, %(track_name)s, %(track_type)s, 
                              %(delta)s, %(gps)s, %(gsm)s, %(inet)s, %(ytube)s, 
                              %(instg)s, %(level)s, %(device_model)s, %(device_id)s)
            """
            y['gps']   = json.dumps(y['gps'])
            y['gsm']   = json.dumps(y['gsm'])
            y['inet']  = json.dumps(y['inet'])
            y['ytube'] = json.dumps(y['ytube'])
            y['instg'] = json.dumps(y['instg'])
            res = cur.execute(sql, y)
            #print(res)
            #res = cur.fetchone()
            #print(res)
            con.commit()
    finally:
        cur.close()
        con.close()

sql = """
select * from (
select 
track_id, track_name, min(dt) as dt_min, max(dt) as dt_max, 
track_type, trim(cast(gsm -> 'operator' as text), '"') as operator,
count(*) as count, count(distinct device_id) as devices,
cast(avg(level) as integer) as level,
avg(cast(gps -> 'lat' as real)) as lat,
avg(cast(gps -> 'lon' as real)) as lon
from data 
group by track_id, track_name, track_type, operator
) as t1 where 1 = 1

"""

def query(sql, filter_dict):
    
    con = ''
    cur = ''
    try:
        con = psycopg2.connect(host = '127.0.0.1', port = '5432', user = 'cyber', password = 'sheep', database = 'cybermeter')
        cur = con.cursor()
        call_args = []
        if 'track_type' in filter_dict: 
            sql += ' and track_type = %s'
            call_args.append(filter_dict['track_type'])
        if 'operator' in filter_dict: 
            sql += ' and lower(operator) = lower(%s)'
            call_args.append(filter_dict['operator'])
        if 'dt_min' in filter_dict: 
            sql += ' and dt_min > %s'
            call_args.append(filter_dict['dt_min'])
        if 'dt_max' in filter_dict: 
            sql += ' and dt_max < %s'
            call_args.append(filter_dict['dt_max'])
        res = cur.execute(sql, call_args)
        print(cur.query)
        #print(res)
        res = cur.fetchall()
        for i in res: print(i)
        con.commit()
    finally:
        cur.close()
        con.close()

if __name__ == '__main__' :
    
    #add_data_from_logs()
    filter_dict = {
        'track_type' : 2,
        'dt_min' : dateutil.parser.parse('2019-11-15T10:00:00.258724'),
        'dt_max' : dateutil.parser.parse('2019-11-18T11:14:37.258724'),
        'operator' : 'megafon'
        }
    query(sql, filter_dict)

    print('done')

