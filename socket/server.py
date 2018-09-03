#coding=utf-8
# 文件名：server.py
import socket

s = socket.socket() #创建scoket对象
host = socket.gethostname() #获取本地主机名
port = 3306
s.bind((host,port))

s.listen(5)
while True:
    c,addr = s.accept()
    print '链接地址：',addr
    c.send('欢迎访问')
    c.close()