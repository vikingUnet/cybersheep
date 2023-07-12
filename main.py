"""
Основной модуль сервера для веб-сервиса cybermeter
"""

import datetime
import json
import falcon
import psycopg2
import logger
import dateutil.parser
from   pprint                import pprint
from   wsgiref.simple_server import make_server
from   psycopg2.extras       import RealDictCursor
from   psycopg2.extensions   import register_adapter

from   falcon_cors import CORS
import webbrowser

log     = logger.log_message
log_err = logger.log_error
bug_err = logger.log_bug

connect_dict = {
    'host'     : '127.0.0.1', 
    'port'     : '5432', 
    'user'     : 'cyber', 
    'password' : 'sheep', 
    'database' : 'cybermeter',
}

def date_handler(val, cur): 
    return str(val)

NewDateteime = psycopg2.extensions.new_type((1114,), 'date_handler', date_handler)
psycopg2.extensions.register_type(NewDateteime)

class PingResource:
    
    def on_get(self, req, resp):
        print('ping_on_get')
        dt = datetime.datetime.now()
        resp.media = date_handler(dt, None)
        
    def on_post(self, req, resp):
        print('ping_on_post')
        dt = datetime.datetime.now()
        resp.media = date_handler(dt, None)

class TrackResource:
    
    def on_get(self, req, resp):
        
        print('TrackResource on_get')
        
        con = ''; cur = ''
        try:
            track_type = req.get_param('track_type', '') or ''
            dt_min     = req.get_param('dt_min',     '') or '' 
            dt_max     = req.get_param('dt_max',     '') or ''
            operator   = req.get_param('operator',   '') or '' 
            if dt_min: dt_min = dateutil.parser.parse(dt_min)
            if dt_max: dt_max = dateutil.parser.parse(dt_max)
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
            call_args = []
            if track_type:  sql += ' and track_type = %s';             call_args.append(track_type)
            if operator:    sql += ' and lower(operator) = lower(%s)'; call_args.append(operator)
            if dt_min:      sql += ' and dt_min >= %s';                call_args.append(dt_min)
            if dt_max:      sql += ' and dt_max <= %s';                call_args.append(dt_max)
            sql += ' order by dt_min'
            con = psycopg2.connect(**connect_dict, cursor_factory = RealDictCursor, )
            cur = con.cursor()
            cur.execute(sql, call_args)
            res = cur.fetchall()
            for row in res:
               row['lat'] = round(row['lat'], 7)
               row['lon'] = round(row['lon'], 7)
            #pprint(res)
            resp.media = res
        except Exception as err:
            print('err:', str(err))
        finally: 
            if cur: cur.close()
            if con: con.close()

class MeasureResource:
    
    def on_get(self, req, resp):
        
        print('MeasureResource on_get')
        
        track_id  = req.get_param('track_id', '')  or ''
        operator  = req.get_param('operator', '')  or ''
        device_id = req.get_param('device_id', '') or ''
        con = ''; cur = ''
        try:
            con = psycopg2.connect(**connect_dict, cursor_factory = RealDictCursor, )
            cur = con.cursor()
            call_args = [track_id]
            sql = """ select * from data where track_id = %s """
            if operator:  
                sql += """ and lower(trim(cast(gsm -> 'operator' as text), '"')) = lower(%s)"""
                call_args.append(operator)
            if device_id: 
                sql += """ and device_id = %s"""
                call_args.append(device_id)
            sql += 'order by dt asc'
            cur.execute(sql, call_args)
            res = cur.fetchall()
            for row in res:
               #buf_dict = {'gps_%s'   % k :        v     for k, v in row['gps'  ].items()}; row.update(buf_dict); row.pop('gps')
                buf_dict = {'gps_%s'   % k :  round(v, 7) for k, v in row['gps'  ].items()}; row.update(buf_dict); row.pop('gps')
                buf_dict = {'gsm_%s'   % k :        v     for k, v in row['gsm'  ].items()}; row.update(buf_dict); row.pop('gsm')
                buf_dict = {'inet_%s'  % k :        v     for k, v in row['inet' ].items()}; row.update(buf_dict); row.pop('inet')
                buf_dict = {'ytube_%s' % k :        v     for k, v in row['ytube'].items()}; row.update(buf_dict); row.pop('ytube')
                buf_dict = {'instg_%s' % k :        v     for k, v in row['instg'].items()}; row.update(buf_dict); row.pop('instg')
            resp.media = res
        except Exception as err:
            print('err:', str(err))
        finally: 
            if cur: cur.close()
            if con: con.close()
            
    def on_post(self, req, resp):
        
        print('MeasureResource on_post')
        
        log(req.media)
        y = req.media
        con = ''; cur = ''
        try:
            con = psycopg2.connect(**connect_dict, cursor_factory = RealDictCursor, )
            cur = con.cursor()
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
            con.commit()
        except Exception as err: print('err:', str(err))
        finally: 
            if cur: cur.close()
            if con: con.close()
        
class BugReportResource:

    def on_post(self, req, resp):
        
        print('BugReportResource on_get')
        
        try: data = req.media; bug_err(data, debug = False)
        except Exception as err: print('err', str(err))

allow_origins_list = ['*'] # ['cyber-map.rumms.ru']
cors       = CORS(allow_origins_list = allow_origins_list, allow_all_origins = True, allow_all_headers = True, allow_all_methods = True)
app        = falcon.API(middleware = [cors.middleware])
ping       = PingResource()
track      = TrackResource()
measure    = MeasureResource()
bug_report = BugReportResource()

app.add_route('/ping',        ping)
app.add_route('/track',       track)
app.add_route('/measure',     measure)
app.add_route('/bug_report',  bug_report)

if __name__ == '__main__':
    
    with make_server('', 8000, app) as httpd:
        print('Serving on port 8000...')
        # Serve until process is killed
        httpd.serve_forever()
