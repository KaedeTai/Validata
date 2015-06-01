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

keys = ['filename', 'version', 'error', 'last', 'size', 'delta']
values  = [argv[key].value if key in argv else '' for key in keys]
keys += ['host']
values += [gethostname()]
sql = 'INSERT INTO `log` (`%s`) VALUES (%s)' % ('`,`'.join(keys), ','.join(['%s'] * len(keys)))
count_sql = 'INSERT INTO `count` (`log_id`,`name`,`hit`)  VALUES (%s,%s,%s)'
group_sql = 'INSERT INTO `group` (`log_id`,`name`,`value`,`hit`)  VALUES (%s,%s,%s,%s)'
print values
try:
    print cur.execute(sql, values)
    log_id = cur.lastrowid
    count = eval(argv['count'].value)
    group = eval(argv['group'].value)
    for name in count:
        print cur.execute(count_sql, (log_id, name, count[name]))
    for name in group:
        for value in group[name]:
          print cur.execute(group_sql, (log_id, name, value, group[name][value]))
    print cur.execute('COMMIT')
except Exception as e:
    print e
db.close()
