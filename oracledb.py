# oracledb.py – a stub so import oracledb never fails
class DatabaseError(Exception): pass
def connect(*args, **kwargs):
    raise DatabaseError("stub!")
