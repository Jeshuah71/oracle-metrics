import os
import sys
import re
import yaml
import oracledb

CONFIG_PATH = os.getenv("ORACLE_CONFIG", "/etc/telegraf/oracle.yml")

def handle_error(err):
    sys.stderr.write(f"ERROR|{err}\n")
    sys.exit(1)

try:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    oracle_cfg = cfg["oracle"]
    USER     = oracle_cfg["user"]
    PASSWORD = oracle_cfg["password"]
    DSN      = oracle_cfg["dsn"]
    INSTANCE = oracle_cfg.get("instance", DSN.split("/")[-1])
    DYNAMIC_METRICS = cfg.get("metrics", [])
except Exception as e:
    handle_error(f"could not load YAML config: {e}")

try:
    conn = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
except oracledb.DatabaseError as e:
    handle_error(e)

class OracleMetrics:
    def __init__(self, conn, instance):
        self.conn     = conn
        self.instance = instance

    def getWaitClassStats(self):
        cur = self.conn.cursor()
        try:
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
        finally:
            cur.close()

    def getWaitStats(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT n.wait_class,
                       n.name,
                       m.wait_count,
                       NVL(ROUND(10*m.time_waited/NULLIF(m.wait_count,0),3),0) avg_ms
                  FROM v$eventmetric m
                  JOIN v$event_name n
                    ON m.event_id = n.event_id
                 WHERE n.wait_class <> 'Idle'
                   AND m.wait_count > 0
                 ORDER BY 1
            """)
            for _wc, wait_name, cnt, avg_ms in cur:
                we = re.sub(r"\s+", "_", wait_name)
                print(f"oracle_wait_event,instance={self.instance},wait_event={we} count={cnt},latency={avg_ms}")
        finally:
            cur.close()

    def getSysmetrics(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT METRIC_NAME, VALUE
                  FROM v$sysmetric
                 WHERE group_id = 2
            """)
            for name, val in cur:
                tag = re.sub(r"\s+", "_", name)
                print(f"oracle_sysmetric,instance={self.instance},metric_name={tag} metric_value={val}")
        finally:
            cur.close()

    def getTableSpaceStats(self):
        cur = self.conn.cursor()
        try:
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
        finally:
            cur.close()

    def getMiscMetrics(self):
        cur = self.conn.cursor()
        try:
            # session counts
            cur.execute("SELECT status, COUNT(*) FROM V$SESSION GROUP BY status")
            for status, cnt in cur:
                print(f"oracle_connectioncount,instance={self.instance},metric_name={status} metric_value={cnt}")
            cur.close()
            # instance & database status
            cur = self.conn.cursor()
            cur.execute("SELECT status, COUNT(*) FROM v$instance GROUP BY status")
            rows = list(cur)
            for s,c in rows:
                if s.upper()=="OPEN":
                    print(f"oracle_status,instance={self.instance},metric_name=instance_status metric_value={c}")
                elif s.upper()=="ACTIVE":
                    print(f"oracle_status,instance={self.instance},metric_name=database_status metric_value={c}")
        finally:
            cur.close()

def run_dynamic_metrics(conn, instance, metrics_cfg):
    for block in metrics_cfg:
        ctx   = block["context"]
        sql   = block["request"]
        tagf  = block.get("fieldtoappend")
        cur   = conn.cursor()
        try:
            cur.execute(sql)
            for row in cur:
                # two‐column case: [value, tag]
                if tagf and len(row)==2:
                    val, tagv = row
                    tags = f"{tagf}={re.sub(r'\\s+','_',str(tagv))}"
                    fields = f"value={val}"
                else:
                    # default: last col value, rest unnamed tags c1,c2…
                    *tags_cols, val = row
                    tags = ",".join(f"c{i}={re.sub(r'\\s+','_',str(v))}"
                                    for i,v in enumerate(tags_cols,1))
                    fields = f"value={val}"
                print(f"oracle_{ctx},instance={instance},{tags} {fields}")
        except Exception as e:
            handle_error(f"{ctx}: {e}")
        finally:
            cur.close()

if __name__ == "__main__":
    om = OracleMetrics(conn, INSTANCE)
    om.getWaitClassStats()
    om.getWaitStats()
    om.getSysmetrics()
    om.getTableSpaceStats()
    om.getMiscMetrics()
    run_dynamic_metrics(conn, INSTANCE, DYNAMIC_METRICS)
