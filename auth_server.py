#!/usr/bin/python
import requests
import json
import socket
import settings
import MySQLdb

# Username and Password based Authentication
# Return Values:
# 	user id if user is authenticated
# 	0       if user is not authenticated
def authenticate(email,password):
    url = "http://localhost:3000/api/auth/login"
    data = {'username': email, 'password': password}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    # Reyurn Success or failure based on response
    if r.status_code == 200:
        db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
        cursor = db.cursor()
        sql="SELECT id FROM users where email='"+email+"'"
        print sql
        cursor.execute(sql)
        db.commit()
        numrows = int(cursor.rowcount)
        for x in range(0,numrows):
            row = cursor.fetchone()
            return row[0]
    else:
        return 0

if __name__ == '__main__':
    print "Starting Authentication Server..."
    s = socket.socket()
    host = settings.auth_server
    port = settings.auth_server_port
    s.bind((host, port))
    s.listen(5)
    print "Listening on port ", port, " at host ", host

    while True:
    	c, addr = s.accept()
    	print 'Got connection from', addr
    	json_data = c.recv(1024)
    	data = json.loads(json_data)
    	username = data["username"]
    	password = data["password"]
    	if authenticate(username,password)>0:
    		c.send('1')
    	else:
    		c.send('0')
    	c.close()
