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
sql = 'SELECT version, size FROM log WHERE filename=%s'
cur.execute(sql, (filename, ))
print 'date\tclose'
for (version, size) in cur:
    print '%s\t%i' % (version, size)

db.close()
