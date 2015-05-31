#!/usr/bin/env python
 
import os
import time
import json
 
from itertools import groupby
from MySQLdb import connect
 
 
def refresh(con):
    cur = con.cursor()
    cur.execute("SELECT address, owner, group FROM instances")
 
    tgs = []
    for key, vals in groupby(cur.fetchall(), key=lambda r: (r[1], r[2])):
        tgs.append({
            labels: dict(zip(['owner', 'group'], key)),
            targets: [t[0] for t in vals],
        })
 
    with open('target_groups.json.new', 'w') as f:
        json.dumps(tgs, f)
        f.flush()
        os.fsync(f.fileno()) 
 
    os.rename('target_groups.json.new', 'target_groups.json')
 
 
if __name__ == '__main__':
    with connect('localhost', '<user>', '<password>', 'testdb') as con:
        while True:
            refresh(con)
            time.sleep(30)
