import struct
import os

# Protocol for sending a message independent of data Size
# Sends message length (in 4 bytes) followed by message data
def send_one_message(sock, data):
    length = len(data)
    sock.sendall(struct.pack('!I', length))
    sock.sendall(data)

# Protocol for sending a file
# Sends file size (in 4 bytes) followed by file data
# Input:
#   Socket, Full path of file to be sent
def send_one_file(sock,file_path):
    statinfo = os.stat(file_path)
    length = statinfo.st_size
    send_one_message(sock,str(length))
    f=open (file_path, "rb") 
    remain=int(length)
    chunksize=1024
    while (remain>0):
        if (remain<chunksize):
            l = f.read(remain)
        else:
            l = f.read(chunksize)
        send_one_message(sock,l)
        remain=remain-chunksize

# Protocol for receiving a message independent of data Size
# Receives message length (in 4 bytes) followed by message data
def recv_one_message(sock):
    lengthbuf = recvall(sock, 4)
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)

# Protocol for receiving a file
# Receives file size (in 4 bytes) followed by file data
# Input:
#   Socket, Full path of file to be sent
def recv_one_file(sock,filename):
    filesize=recv_one_message(sock)
    open(filename, 'w').close()
    chunksize=1024
    remain=int(filesize)
    while (remain>0):
        l = recv_one_message(sock)
        with open(filename, "a") as f:
            f.write(l)
        remain=remain-chunksize

# Receives data
# Input:
#   Socket, Number of bytes to receive
def recvall(sock, count):
    buf = b''
    while count:
        newbuf = sock.recv(count)
        if not newbuf: return None
        buf += newbuf
        count -= len(newbuf)
    return buf
