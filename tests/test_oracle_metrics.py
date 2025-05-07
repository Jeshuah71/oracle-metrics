# -------------------------------------------------------------------
# tests/test_oracle_metrics.py
# -------------------------------------------------------------------
import re
import pytest
import oracle_metrics
import oracledb   # your local stub

class DummyCursor:
    def __init__(self, mapping, conn):
        self._mapping = mapping
        self._conn     = conn
        self._rows     = []

    def execute(self, sql):
        # remember the SQL in case OracleMetrics ever inspects it
        self._conn._last_sql = sql
        # now pick the matching fake rows for this SQL
        for frag, rows in self._mapping.items():
            if frag in sql:
                self._rows = rows
                break
        else:
            self._rows = []

    def __iter__(self):
        return iter(self._rows)

    def close(self):
        pass


class DummyConnection:
    def __init__(self, mapping):
        self._mapping = mapping
        self._last_sql = ""

    def cursor(self):
        # always return a fresh DummyCursor
        return DummyCursor(self._mapping, self)

    def close(self):
        pass


@pytest.fixture(autouse=True)
def fake_connect(monkeypatch):
    mapping = {
        "v$waitclassmetric":           [("User I/O", 0.123)],
        "v$eventmetric":               [("App", "evt", 5, 2.5)],
        "v$sysmetric":                 [("Host CPU Utilization (%)", 9.87)],
        "dba_tablespace_usage_metrics":[("USERS",10,90,100,10.0)],
        "V$SESSION":                   [("ACTIVE",3)],
        "v$instance":                  [("OPEN",1),("ACTIVE",1)],
    }

    def _fake_connect(*args, **kwargs):
        return DummyConnection(mapping)

    # patch the same oracledb module that oracle_metrics imported
    monkeypatch.setattr(oracledb, "connect", _fake_connect)


def test_all_metrics_print(capsys):
    class Args: pass
    args = Args()
    args.user     = "u"
    args.password = "p"
    args.dsn      = "d"
    args.instance = "T1"
    args.pfile    = None

    m = oracle_metrics.OracleMetrics(args)
    m.getWaitClassStats()
    m.getWaitStats()
    m.getSysmetrics()
    m.getTableSpaceStats()
    m.getMiscMetrics()

    out = capsys.readouterr().out
    assert "oracle_wait_class,instance=T1,wait_class=User_I/O"                in out
    assert "wait_event=evt" in out
    assert "oracle_sysmetric,instance=T1,metric_name=Host_CPU_Utilization_(%)" in out
    assert "oracle_tablespaces,instance=T1,tbs_name=USERS"                    in out
    assert "oracle_connectioncount,instance=T1"                               in out
    assert "oracle_status,instance=T1,metric_name=instance_status"            in out
