import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysettings")

# 2) initialize Django
import django
django.setup()

# 3) now we can import dbconfig and pull our creds
import dbconfig
cred = dbconfig.get("oracle")
USER     = cred["user"]
PASSWORD = cred["password"]
DSN      = cred["dsn"]
INSTANCE = cred.get("instance", "")

import re
import sys
import oracledb


def handle_error(err):
    """Print Telegraf-style ERROR|<msg> and exit nonzero."""
    sys.stderr.write(f"ERROR|{err}\n")
    sys.exit(1)


class OracleMetrics:
    def __init__(self):
        self.instance = INSTANCE
        try:
            self.connection = oracledb.connect(
                user=USER,
                password=PASSWORD,
                dsn=DSN
            )
        except oracledb.DatabaseError as e:
            handle_error(e)

    def getWaitClassStats(self):
        cur = None
        try:
            cur = self.connection.cursor()
            cur.execute("""
                SELECT n.wait_class,
                       ROUND(m.time_waited/m.INTSIZE_CSEC,3) AAS
                  FROM v$waitclassmetric m
                  JOIN v$system_wait_class n
                    ON m.wait_class_id = n.wait_class_id
                 WHERE n.wait_class != 'Idle'
            """)
            for wait_name, wait_value in cur:
                tag = re.sub(r"\s+", "_", wait_name)
                print(f"oracle_wait_class,instance={self.instance},wait_class={tag} wait_value={wait_value}")
        except Exception as e:
            handle_error(e)
        finally:
            if cur: cur.close()

    def getWaitStats(self):
        cur = None
        try:
            cur = self.connection.cursor()
            cur.execute("""
                SELECT n.wait_class,
                       n.name,
                       m.wait_count,
                       NVL(ROUND(10*m.time_waited/NULLIF(m.wait_count,0),3),0) avg_ms
                  FROM v$eventmetric m
                  JOIN v$event_name   n
                    ON m.event_id = n.event_id
                 WHERE n.wait_class <> 'Idle'
                   AND m.wait_count  > 0
                 ORDER BY 1
            """)
            for _wc, wait_name, cnt, avg_ms in cur:
                we = re.sub(r"\s+", "_", wait_name)
                print(f"oracle_wait_event,instance={self.instance},wait_event={we} count={cnt},latency={avg_ms}")
        except Exception as e:
            handle_error(e)
        finally:
            if cur: cur.close()

    def getSysmetrics(self):
        cur = None
        try:
            cur = self.connection.cursor()
            cur.execute("""
                SELECT METRIC_NAME, VALUE
                  FROM v$sysmetric
                 WHERE group_id = 2
            """)
            for name, val in cur:
                tag = re.sub(r"\s+", "_", name)
                print(f"oracle_sysmetric,instance={self.instance},metric_name={tag} metric_value={val}")
        except Exception as e:
            handle_error(e)
        finally:
            if cur: cur.close()

    def getTableSpaceStats(self):
        cur = None
        try:
            cur = self.connection.cursor()
            cur.execute("""
                SELECT tablespace_name,
                       ROUND(used_space) used_mb,
                       ROUND(max_size - used_space) free_mb,
                       ROUND(max_size) max_mb,
                       ROUND(used_space*100/max_size,2) pct_used
                  FROM (
                    SELECT m.tablespace_name,
                           m.used_space * t.block_size/1024/1024 used_space,
                           (CASE WHEN t.bigfile='YES'
                             THEN POWER(2,32)*t.block_size/1024/1024
                             ELSE tablespace_size*t.block_size/1024/1024
                            END) max_size
                      FROM dba_tablespace_usage_metrics m
                      JOIN dba_tablespaces t
                        ON m.tablespace_name = t.tablespace_name
                  )
            """)
            for name, used, free, maximum, pct in cur:
                tag = re.sub(r"\s+", "_", name)
                print(
                    f"oracle_tablespaces,instance={self.instance},"
                    f"tbs_name={tag} used_space_mb={used},"
                    f"free_space_mb={free},percent_used={pct},"
                    f"max_size_mb={maximum}"
                )
        except Exception as e:
            handle_error(e)
        finally:
            if cur: cur.close()

    def getMiscMetrics(self):
        try:
            # session counts
            cur = self.connection.cursor()
            cur.execute("SELECT status, COUNT(*) FROM V$SESSION GROUP BY status")
            for status, cnt in cur:
                print(f"oracle_connectioncount,instance={self.instance},metric_name={status} metric_value={cnt}")
            cur.close()

            # instance & db status
            cur = self.connection.cursor()
            cur.execute("SELECT status, COUNT(*) FROM v$instance GROUP BY status")
            rows = list(cur)
            cur.close()

            open_val   = next((c for s,c in rows if s.upper()=="OPEN"),   0)
            active_val = next((c for s,c in rows if s.upper()=="ACTIVE"), 0)

            print(f"oracle_status,instance={self.instance},metric_name=instance_status metric_value={open_val}")
            print(f"oracle_status,instance={self.instance},metric_name=database_status metric_value={active_val}")

        except Exception as e:
            handle_error(e)


if __name__ == "__main__":
    m = OracleMetrics()
    m.getWaitClassStats()
    m.getWaitStats()
    m.getSysmetrics()
    m.getTableSpaceStats()
    m.getMiscMetrics()
