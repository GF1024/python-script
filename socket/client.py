#coding=utf-8
# 文件名：client.py

import socket

s = socket.socket()
host =socket.gethostname()
port = 3306

s.connect((host,port))
print s.recv(1024)
s.close()