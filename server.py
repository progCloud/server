import socket
import protocol
import sys
from thread import *
import json
import requests
import settings
import auth_server
import datetime
import time
import os
import random
import MySQLdb
import shutil

# Returns folder name given folder id
# Returns empty string if folder not present
def folder_name(folder_id):
    if folder_id == 0:
        return ''
    # Connect to MySQL Database
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    sql="SELECT count(*),name FROM folders where id='"+str(folder_id)+"'"
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        if(row[0]==0):
            return ''
        else:
            return row[1]

# Resurns the list of shared folders for user with id 'userid'
def get_shared_folders_list(userid):
    # Connect to MySQL Database
    shared_folder_ids_list = []
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    sql='SELECT count(*),folder_id FROM shared_folders where shared_user_id='+str(userid)
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        shared_folder_ids_list.append(int(row[1]))
    return shared_folder_ids_list

# Updated database for given file details
def update_file_to_db(filename,filesize,timestamp,userid,mimetype):
    # Connect to MySQL Database
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    sql="SELECT count(*),id FROM assets where user_id='"+str(userid)+"'and uploaded_file_file_name='"+filename+"'"
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    type_file=''
    file_content=mimetype
    fileid=0
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    for x in range(0,numrows):
        row = cursor.fetchone()
        print row[0]==0
        if(row[0]==0):
            type_file='insert'
        else:
            type_file='update'
            fileid=row[1]

    # Insert or update database record for given file details
    if type_file=='insert':
        sql="INSERT INTO `assets`(`user_id`, `created_at`, `updated_at`, `uploaded_file_file_name`, `uploaded_file_content_type`, `uploaded_file_file_size`) VALUES ('"+str(userid)+"','"+str(timestamp)+"','"+str(timestamp)+"','"+filename+"','"+file_content+"','"+str(filesize)+"')"
        print sql
    elif type_file=='update':
        sql="UPDATE `assets` SET `updated_at`='"+str(timestamp)+"',`uploaded_file_file_name`='"+filename+"',`uploaded_file_content_type`='"+file_content+"',`uploaded_file_file_size`='"+str(filesize)+"' where user_id='"+str(userid)+"'and uploaded_file_file_name='"+filename+"' and id="+str(fileid)
        print sql
    else:
        print 'Error in database query in update_file_to_db'
    cursor.execute(sql)
    db.commit()
    if (type_file=='insert'):
        return cursor.lastrowid
    else:
        return fileid

def remove_file_from_db(filename,userid):
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    #Execute SQL select statement
    sql="SELECT id FROM assets where user_id='"+str(userid)+"'and uploaded_file_file_name='"+filename+"'"
    cursor.execute(sql)
    db.commit()
    numrows=int(cursor.rowcount)
    if numrows == 0 :
	return "NotExist"
    elif numrows > 1:
	return "Multiple"
    else:
	row=cursor.fetchone()
	sql="DELETE FROM assets where id="+str(row[0])
	print sql
	cursor.execute(sql)
	db.commit()
	return str(row[0])

# Returns list of files to be sent back to client
# Return Value:
#   list_name,list_id,list_size, list_folder
def files_to_sync_back(response,userid):
    client_files_list=[]
    client_timestamp_list=[]
    list_name=[]
    list_id=[]
    list_size=[]
    list_folder = []
    for i in response:
        client_files_list.append(i['filename'])
        client_timestamp_list.append(i['timestamp'])
    # connect
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # execute SQL select statement
    shared_folder_ids_list = get_shared_folders_list(userid)
    placeholder= '?'
    placeholders= ', '.join(placeholder for unused in shared_folder_ids_list)
    print shared_folder_ids_list
    sql='SELECT uploaded_file_file_name,updated_at,id,uploaded_file_file_size, folder_id FROM assets WHERE user_id='+str(userid)+' OR folder_id IN ( ' + ','.join(map(str, shared_folder_ids_list)) + ' )' #% str(tuple(shared_folder_ids_list))  #placeholders          # ' + ','.join(map(str, shared_folder_ids_list)) + '
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        print row[0]
        try:
            from datetime import datetime as dt
            b=dt.strptime(str(client_timestamp_list[client_files_list.index(row[0])]), "%Y-%m-%d %H:%M:%S")
            a=dt.strptime(str(row[1]), "%Y-%m-%d %H:%M:%S")
            
            if (a!=b):
                list_name.append(row[0])
                list_id.append(row[2])
                list_size.append(row[3])
                list_folder.append(folder_name(int(row[4] or 0)))
        except ValueError:
            list_name.append(row[0])
            list_id.append(row[2])
            list_size.append(row[3])
            list_folder.append(folder_name(int(row[4] or 0)))
    return list_name,list_id,list_size, list_folder


# Thread for client connection
def clientthread(conn):
    reply = protocol.recv_one_message(conn)
    data = json.loads(reply)
    
    #Temporarily Bypassing auth server (do not have rails :/)
    userid = auth_server.authenticate(data['email'],data['password'])
    #userid=1

    req_type = data["req_type"]

    if userid>0:
        protocol.send_one_message(conn, '1')
        if req_type == 'add_file':
        # Receive file from client
            filename_r = data["filename"]
            filesize = data['file_size']
            timestamp = data['file_timestamp']
            mimetype = data['content_type']
            file_id = update_file_to_db(filename_r,filesize,timestamp,userid,mimetype)
            directory=settings.server_files_folder+str(file_id)+'/'
            if not os.path.exists(directory):
                os.makedirs(directory)
            file_server_location=directory+filename_r
            protocol.recv_one_file(conn,file_server_location)
        elif req_type == 'pull':
            print 'Pull Request Received'
            client_dir_parse_json = data['dir_parse_json']
            # Sync back files to client
            filename_list,id_list,file_size_list, folder_list=files_to_sync_back(client_dir_parse_json,userid)
            print filename_list
            print folder_list
            protocol.send_one_message(conn,str(len(filename_list)))
            protocol.send_one_message(conn, json.dumps({'filename_list' : filename_list,'filesize_list':file_size_list, 'foldername_list':folder_list}))
            # file_location=''
            for x in range(0,len(filename_list)):
                file_location=settings.server_files_folder+str(id_list[x])+'/'+str(filename_list[x])
                if (os.path.isfile(file_location)):
                    protocol.send_one_message(conn,'1')
                    protocol.send_one_file(conn,file_location)
                else:
                    protocol.send_one_message(conn,'0')
        elif req_type == 'delete_file':
            filename = data["filename"]
            file_id = remove_file_from_db(filename,userid)
            if file_id == "NotExist":
                protocol.send_one_message(conn,"DoesNotExist")
            elif file_id == "Multiple":
                protocol.send_one_message(conn,"InternalError")
            else:
                directory=settings.server_files_folder+str(file_id)+'/'
                print directory
                shutil.rmtree(directory)
                protocol.send_one_message(conn,'Success')
    else:
        protocol.send_one_message(conn,'0')
        conn.close()


# Starts listening on port specified in settings and creates a new thread for each client connection
if __name__ == '__main__': 
    HOST = settings.file_server             # Symbolic name meaning all available interfaces
    PORT = settings.file_server_port        # Arbitrary non-privileged port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Socket Created'
    try:
        s.bind((HOST, PORT))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    print 'Socket bind complete'
    s.listen(10)
    print 'Socket now listening...'
    while 1:
        conn, addr = s.accept()
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        start_new_thread(clientthread ,(conn,))
 
