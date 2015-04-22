import socket
import sys
from thread import *
import csv
import parse
#from tabulate import tabulate
 
def clientthread(conn):
    flag=1
    flag1=1
    conn.send('Welcome to the server.\n') 
    
    #conn.send('\nEnter the file path with name(eg.:- /home/actor.csv)')  
   
    #print filepath
    conn.send('Enter the query:\n') 
    while True:
    	f = open(filepath,"rb")
        cr = csv.reader(f)
        header = cr.next()
        data = conn.recv(1024)
        
        #print condi
        if data=='break':
        	break
        tok = parse.test(data)
        if type(tok)!=dict:
        	conn.send(str(tok))
        	f.close()
	elif tok['tables'][0]!=filename:
		conn.send("wrong table\n")
		f.close()
	else:
		if tok['columns']!='*':
			for col in tok['columns']:
				if col not in header:
					conn.send("wrong column name "+col+"\n")
					flag=0
					break
			if flag==1:
				f.close()
				f = open(filepath,"rb")
				cr = csv.DictReader(f)
				
				reply = ''
		
				for col in tok['columns']:
						reply = reply + col + "\t|\t"
				reply =reply + "\n"
				conn.sendall(reply)
				i = data.find('where')
				condi = data[(i+6):]
				for row in cr:
					print row
					for k,v in row.iteritems():
				    		if v.isdigit()!=True:
				    			exec("%s='%s'" % (k,v))
						else:
							exec("%s=%d" % (k,int(v)))
					#print Age+100
					if i!=-1:
						
						if eval(condi)==False:
							#print 'hi'
							flag1 = 0
						
       					if flag1==1:
						reply = ''
						for col in tok['columns']:
							reply = reply + row[col] + "\t|\t"
						reply =reply + "\n"
						conn.send(reply)
					flag1=1
				f.close()
			flag = 1
		
		else:
			f.close()
			f = open(filepath,"rb")
			cr = csv.reader(f)
			'''
			reply = ''
		
			for col in tok['columns']:
				reply = reply + col + "\t|\t"
			reply =reply + "\n"
			conn.sendall(reply)
			'''
			for row in cr:
				reply = ''
				for col in row:
					reply = reply + col + "\t|\t"
				reply =reply + "\n"
				conn.send(reply)
					
			f.close()	
					
    conn.close()
  
 
HOST = ''  
PORT = int(input("Enter the port no.-"))
filename = str(input('Enter the file name\n'))
filepath = str(input('Enter the file path\n')) 
 
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
