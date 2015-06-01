#!/usr/bin/env python
 
import os
import time
import json
 
from itertools import groupby
from MySQLdb import connect
 
 
def refresh(cur):
    res = cur.execute("SELECT address, job, zone FROM instances")
 
    tgs = []
    for key, vals in groupby(cur.fetchall(), key=lambda r: (r[1], r[2])):
        tgs.append({
            'labels': dict(zip(['job', 'zone'], key)),
            'targets': [t[0] for t in vals],
        })
 
    with open('tgroups/target_groups.json.new', 'w') as f:
        json.dump(tgs, f)
        f.flush()
        os.fsync(f.fileno()) 
 
    os.rename('tgroups/target_groups.json.new', 'tgroups/target_groups.json')
 
 
if __name__ == '__main__':
    while True:
        with connect('localhost', 'root', '', 'test') as cur:
            refresh(cur)
        time.sleep(30)
