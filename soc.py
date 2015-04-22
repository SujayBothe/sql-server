import socket
import sys
from thread import *
import csv
import parse
#from tabulate import tabulate
 
def clientthread(conn):
    
    conn.send('Welcome to the server. Type the query and hit enter\n') 
    #l = []
    while True:
        cr = csv.reader(open("/home/sujay/Desktop/actor.csv","rb"))
        data = conn.recv(1024)
        tokens = parse.test(data)
	print tokens
	'''
	for row in cr:
		reply = row[0]+"\t"+row[1]+"\t"+row[2]+"\n"
		#l.append(row)
		conn.sendall(reply)
        # print tabulate(l, headers=['Title','Release Date','Director'])
        if data=='break\n': 
            break
        '''
    conn.close()
  
 
HOST = ''   
PORT = 8889 
 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'
 

try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
     
print 'Socket bind complete'
 
s.listen(1)
print 'Socket now listening'
   
   
while 1:
   
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    start_new_thread(clientthread ,(conn,))
 
s.close()



'''

might requierd in future

import csv
import sys

f = open(sys.argv[1], 'rt')
try:
    reader = csv.reader(f)
    for row in reader:
        print row
finally:
    f.close()
    '''
