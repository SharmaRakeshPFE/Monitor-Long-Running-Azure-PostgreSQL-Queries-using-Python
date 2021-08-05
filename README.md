# Monitor-Long-Running-Azure-PostgreSQL-Queries-using-Python
#Create a CSV for all the long running queries using Python for Azure Database for PostgreSQL Database#
import psycopg2
import csv
import pandas as pd
import io

conn = psycopg2.connect(user="rakesh_123@infypostgresql",
                                  password="Cxxxxxx7",
                                  host="infxxxxxx.postgres.database.azure.com",
                                   database="rakesh_azure")
conn.autocommit = True

sql_query = pd.read_sql_query(''' 
                                SELECT blocked_locks.pid         AS blocked_pid,
       blocked_activity.usename  AS blocked_user,
	   now() - blocked_activity.query_start
		                         AS blocked_duration,
       blocking_locks.pid        AS blocking_pid,
       blocking_activity.usename AS blocking_user,
	   now() - blocking_activity.query_start
                                 AS blocking_duration,
       blocked_activity.query    AS blocked_statement,
       blocking_activity.query   AS blocking_statement
FROM pg_catalog.pg_locks AS blocked_locks
JOIN pg_catalog.pg_stat_activity AS blocked_activity
    ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks AS blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity AS blocking_activity
    ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted
'''
                              ,conn)
df = pd.DataFrame(sql_query)
df.to_csv (r'C:\temp\Blocking.csv', index = False) 
