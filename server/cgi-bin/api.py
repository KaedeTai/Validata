#!/home/y/bin64/python2.7
from cgi import FieldStorage
from socket import gethostname
import sys
import MySQLdb

print 'Content-Type: text/plain'
print

argv = FieldStorage()
db = MySQLdb.connect(user='root', db='validata')
cur = db.cursor()

def exit(msg):
    print msg
    sys.exit()

keys = ['filename', 'version', 'size', 'delta', 'error']
values  = [argv[key].value if key in argv else '' for key in keys]
keys += ['host']
values += [gethostname()]
sql = 'INSERT INTO log (%s) VALUES (%s)' % (','.join(keys), ','.join(['%s'] * len(keys)))
print values
try:
    print cur.execute(sql, values)
    print cur.execute('COMMIT')
except Exception as e:
    print e
db.close()
