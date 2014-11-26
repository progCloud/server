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


# Returns folder_id for given foldername if it exists as shared folder or user's shared folder
# If it does not exist, create the folder for user and return new folder's id
def get_or_make_folder_id(userid, folder_name):
    folder_id = -1
    if folder_name == '.':
        return -1
    # Connect to MySQL Database
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    sql="SELECT id FROM folders where user_id="+str(userid)+" and name='"+ str(folder_name) + "'"
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        folder_id = int(row[0])
    if folder_id == -1:
        sql="SELECT folder_id FROM shared_folders where shared_user_id="+str(userid)+" and folder_name='"+ str(folder_name) + "'"
        print sql
        cursor.execute(sql)
        db.commit()
        numrows = int(cursor.rowcount)
        for x in range(0,numrows):
            row = cursor.fetchone()
            folder_id = int(row[0])
    if folder_id == -1:
        # Create new folder entry
        sql="INSERT INTO `folders`(`name`, `user_id`) VALUES ('"+str(folder_name)+"','"+str(userid)+"')"
        cursor.execute(sql)
        db.commit()
        folder_id = get_or_make_folder_id(userid, folder_name)
    return folder_id

# Resurns the list of shared folders for user with id 'userid'
def get_shared_folders_list(userid):
    # Connect to MySQL Database
    shared_folder_ids_list = []
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    sql='SELECT folder_id FROM shared_folders where shared_user_id='+str(userid)
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        shared_folder_ids_list.append(int(row[0]))
        print row[0]
    return shared_folder_ids_list

# Deletes Folder
def delete_folder(userid, foldername):
    # Connect to MySQL Database
    folder_id = get_or_make_folder_id(userid, foldername)
    shared_folders_list = get_shared_folders_list(userid)
    if shared_folders_list.count(folder_id) == 1:
        db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
        cursor = db.cursor()
        # Execute SQL select statement
        sql="DELETE FROM shared_folders where folder_id="+str(folder_id)+" and shared_user_id = " + str(userid)
        cursor.execute(sql)
        db.commit()
        # Folder is shared
    else:
        db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
        cursor = db.cursor()
        # Execute SQL select statement
        sql="DELETE FROM folders where id="+str(folder_id)
        cursor.execute(sql)
        db.commit()
        sql="DELETE FROM shared_folders where folder_id="+str(folder_id)
        cursor.execute(sql)
        db.commit()
    return 1

# Updated database for given file details
def update_file_to_db(filename,foldername,filesize,timestamp,userid,mimetype):
    # Connect to MySQL Database
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    if foldername == '.':
        folder_id = -1
        sql="SELECT count(*),id FROM assets where user_id='"+str(userid)+"'and uploaded_file_file_name='"+filename+"' and folder_id = -1"
    else:
        folder_id = get_or_make_folder_id(userid, foldername)
        sql="SELECT count(*),id FROM assets where uploaded_file_file_name='"+filename+"' and folder_id ="+str(folder_id)
    print sql
    # Execute SQL select statement
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
        if folder_id == -1:
            sql="INSERT INTO `assets`(`user_id`, `created_at`, `updated_at`, `uploaded_file_file_name`, `uploaded_file_content_type`, `uploaded_file_file_size`, `folder_id`) VALUES ('"+str(userid)+"','"+str(timestamp)+"','"+str(timestamp)+"','"+filename+"','"+file_content+"','"+str(filesize)+"', '-1')"
        else:
            sql="INSERT INTO `assets`(`user_id`, `folder_id`, `created_at`, `updated_at`, `uploaded_file_file_name`, `uploaded_file_content_type`, `uploaded_file_file_size`) VALUES ('"+str(userid)+"','"+str(folder_id)+"','"+str(timestamp)+"','"+str(timestamp)+"','"+filename+"','"+file_content+"','"+str(filesize)+"')"
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

def remove_file_from_db(filename,foldername,userid):
    folder_id = get_or_make_folder_id(userid, foldername)
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # Execute SQL select statement
    if folder_id == -1:
        sql="SELECT id FROM assets where user_id='"+str(userid)+"'and uploaded_file_file_name='" + filename + "' and folder_id=" + str(-1)
    else:
        sql="SELECT id FROM assets where uploaded_file_file_name='" + filename + "' and folder_id=" + str(folder_id)
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
    client_folder_list=[]
    client_timestamp_list=[]
    list_name=[]
    list_id=[]
    list_size=[]
    list_folder = []
    for i in response:
        client_files_list.append(i['filename'])
        client_timestamp_list.append(i['timestamp'])
        client_folder_list.append(i['foldername'])
    # connect
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # execute SQL select statement
    shared_folder_ids_list = get_shared_folders_list(userid)
    placeholder= '?'
    placeholders= ', '.join(placeholder for unused in shared_folder_ids_list)
    shared_folder_ids_list.append(-1)
    sql='SELECT uploaded_file_file_name,updated_at,id,uploaded_file_file_size, folder_id FROM assets WHERE user_id='+str(userid)+' OR folder_id IN ( ' + ','.join(map(str, shared_folder_ids_list)) + ' )'
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        print row[0]
        from datetime import datetime as dt
        toCopy=0
        myIndex = -1
        for i,clFile in enumerate(client_files_list):
            if clFile == row[0] :
                if client_folder_list[i]=='.':
                    if row[4] == -1:
                        myIndex=i
                else:
                    if folder_name(row[4]) == client_folder_list[i]:
                        myIndex = i

        if (myIndex == -1 ):
            toCopy=1
        else:  
            b=dt.strptime(str(client_timestamp_list[myIndex]), "%Y-%m-%d %H:%M:%S")
            a=dt.strptime(str(row[1]), "%Y-%m-%d %H:%M:%S")
            if (a>b):
                toCopy=1

        if toCopy == 1:
            list_name.append(row[0])
            list_id.append(row[2])
            list_size.append(row[3])
            fname = folder_name(row[4])
            if fname == '' :
                fname = '.'
            list_folder.append(fname)

    
    return list_name,list_id,list_size, list_folder


# Returns list of files to get from client
# Return Value:
#   list_name,list_id,list_size, list_folder
def files_to_get(response,userid):
    client_files_list=[]
    client_folder_list=[]
    client_timestamp_list=[]
    list_name=[]
    list_folder = []
    for i in response:
        client_files_list.append(i['filename'])
        client_timestamp_list.append(i['timestamp'])
        client_folder_list.append(i['foldername'])
    list_name = client_files_list
    list_folder = client_folder_list
    # connect
    db = MySQLdb.connect(host=settings.db_host, user=settings.db_user, passwd=settings.db_passwd,db=settings.db_db)
    cursor = db.cursor()
    # execute SQL select statement
    shared_folder_ids_list = get_shared_folders_list(userid)
    placeholder= '?'
    placeholders= ', '.join(placeholder for unused in shared_folder_ids_list)
    shared_folder_ids_list.append(-1)
    sql='SELECT uploaded_file_file_name,updated_at,id,uploaded_file_file_size, folder_id FROM assets WHERE user_id='+str(userid)+' OR folder_id IN ( ' + ','.join(map(str, shared_folder_ids_list)) + ' )'
    print sql
    cursor.execute(sql)
    db.commit()
    numrows = int(cursor.rowcount)
    for x in range(0,numrows):
        row = cursor.fetchone()
        print row[0]
        from datetime import datetime as dt
        toNotCopy=1
        myIndex = -1
        for i,clFile in enumerate(client_files_list):
            if clFile == row[0] :
                if client_folder_list[i]=='.':
                    if row[4] == -1:
                        myIndex=i
                else:
                    if folder_name(row[4]) == client_folder_list[i]:
                        myIndex = i
        if (myIndex == -1):
            toNotCopy=0
        else:  
            b=dt.strptime(str(client_timestamp_list[myIndex]), "%Y-%m-%d %H:%M:%S")
            a=dt.strptime(str(row[1]), "%Y-%m-%d %H:%M:%S")
            if (a > b):
                toNotCopy=0

        if toNotCopy == 1:
            list_name.remove(row[0])
            fname = folder_name(row[4])
            if fname == '' :
                fname = '.'
            list_folder.remove(fname)

    return list_name, list_folder


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
            foldername_r = data['foldername']
            file_id = update_file_to_db(filename_r,foldername_r,filesize,timestamp,userid,mimetype)
            directory=settings.server_files_folder+str(file_id)+'/'
            if not os.path.exists(directory):
                os.makedirs(directory)
            file_server_location=directory+filename_r
            protocol.recv_one_file(conn,file_server_location)
            protocol.send_one_message(conn, 'Success')
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
                print file_location
                if (os.path.isfile(file_location)):
                    protocol.send_one_message(conn,'1')
                    protocol.send_one_file(conn,file_location)
                else:
                    protocol.send_one_message(conn,'0')
                    print id_list[x]
        elif req_type == 'push':
            print 'Push Request Received'
            client_dir_parse_json = data['dir_parse_json']
            # Sync back files to client
            filename_list, folder_list=files_to_get(client_dir_parse_json,userid)
            print filename_list
            print folder_list
            protocol.send_one_message(conn,str(len(filename_list)))
            protocol.send_one_message(conn, json.dumps({'filename_list' : filename_list,'foldername_list':folder_list}))
            # file_location=''
            #for x in range(0,len(filename_list)):
                #file_location=settings.server_files_folder+str(id_list[x])+'/'+str(filename_list[x])
                #protocol.recv_one_file(conn,file_location)
        elif req_type == 'delete_file':
            filename = data["filename"]
            foldername = data["foldername"]
            file_id = remove_file_from_db(filename,foldername,userid)
            if file_id == "NotExist":
                protocol.send_one_message(conn,"DoesNotExist")
            elif file_id == "Multiple":
                protocol.send_one_message(conn,"InternalError")
            else:
                directory=settings.server_files_folder+str(file_id)+'/'
                print directory
                shutil.rmtree(directory)
                protocol.send_one_message(conn,'Success')
        elif req_type == 'add_folder':
            foldername = data["foldername"]
            folder_id = get_or_make_folder_id(userid, foldername)
            if folder_id > 0:
                protocol.send_one_message(conn,'Success')
            else:
                protocol.send_one_message(conn,'Failure')
        elif req_type == 'delete_folder':
            print 'xxx'
            foldername = data["foldername"]
            status = delete_folder(userid, foldername)
            print 'xxx'
            if status > 0:
                protocol.send_one_message(conn,'Success')
            else:
                protocol.send_one_message(conn,'Failure')
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
