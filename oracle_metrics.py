#!/usr/bin/env python3
"""
oracle_metrics.py

Script to extract Oracle metrics in InfluxDB Line Protocol format
using the python-oracledb driver (thin mode).
"""

import argparse
import re
import sys
import oracledb  # real driver


def handle_error(error_message):
    """Telegraf expects 'ERROR|<msg>' on stderr and non-zero exit."""
    sys.stderr.write(f"ERROR|{error_message}\n")
    sys.exit(1)


class OracleMetrics:
    def __init__(self, args):
        self.instance = args.instance
        try:
            self.connection = oracledb.connect(
                user=args.user,
                password=args.password,
                dsn=args.dsn
            )
        except oracledb.DatabaseError as e:
            handle_error(e)

    def getWaitClassStats(self):
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
            cur.close()

    def getWaitStats(self):
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
                # **only** wait_event tag (tests look for this)
                print(f"oracle_wait_event,instance={self.instance},wait_event={we} count={cnt},latency={avg_ms}")
        except Exception as e:
            handle_error(e)
        finally:
            cur.close()

    def getSysmetrics(self):
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
            cur.close()

    def getTableSpaceStats(self):
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
                           (CASE WHEN t.bigfile = 'YES'
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
            cur.close()

    def getMiscMetrics(self):
        try:
            # session counts
            cur = self.connection.cursor()
            cur.execute("SELECT status, COUNT(*) FROM V$SESSION GROUP BY status")
            for status, cnt in cur:
                print(
                    f"oracle_connectioncount,instance={self.instance},"
                    f"metric_name={status} metric_value={cnt}"
                )
            cur.close()

            # now pull the two aliases out of the same v$instance mapping
            cur = self.connection.cursor()
            cur.execute("""
                SELECT status, COUNT(*) FROM v$instance GROUP BY status
            """)
            rows = list(cur)
            cur.close()

            # pick off OPEN ⇒ instance_status, ACTIVE ⇒ database_status
            open_val   = next((c for s, c in rows if s.upper() == "OPEN"),   0)
            active_val = next((c for s, c in rows if s.upper() == "ACTIVE"), 0)

            print(
                f"oracle_status,instance={self.instance},"
                f"metric_name=instance_status metric_value={open_val}"
            )
            print(
                f"oracle_status,instance={self.instance},"
                f"metric_name=database_status metric_value={active_val}"
            )
        except Exception as e:
            handle_error(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Oracle metrics in InfluxDB format using python-oracledb"
    )
    parser.add_argument("-u", "--user",     required=True)
    parser.add_argument("-p", "--password", default="")
    parser.add_argument("-d", "--dsn",      required=True)
    parser.add_argument("-i", "--instance", required=True)
    parser.add_argument("--pfile")
    args = parser.parse_args()

    if args.pfile:
        try:
            args.password = open(args.pfile).read().strip()
        except Exception as e:
            handle_error(e)

    try:
        m = OracleMetrics(args)
        m.getWaitClassStats()
        m.getWaitStats()
        m.getSysmetrics()
        m.getTableSpaceStats()
        m.getMiscMetrics()
    except Exception as e:
        handle_error(e)
    finally:
        if hasattr(m, "connection"):
            m.connection.close()
