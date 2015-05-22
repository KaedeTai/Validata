#!/usr/bin/python
from cgi import FieldStorage
from socket import gethostname
import sys
import MySQLdb

print 'Content-Type: text/plain'
print

argv = FieldStorage()
db = MySQLdb.connect(user='root', db='validata')
cur = db.cursor()

filename = argv['filename'].value
#filename = 'qlas_data/tw/tmp/base-movie'
sql = 'SELECT version, size, delta, error FROM log WHERE filename=%s'
cur.execute(sql, (filename, ))
print 'Version\t\t\tSize\tDelta\tError'
for (version, size, delta, error) in cur:
    print '%s\t%i\t%s\t%i' % (version, size, 'valid' if delta == 0 else 'alert' if delta ** 2 == 1 else 'error', error)

db.close()
