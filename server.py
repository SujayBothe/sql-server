import socket
import sys
from thread import *
import csv
import parse

def check(row,i,condi,conn):
	for k,v in row.iteritems():
		if v.isdigit()!=True:
			exec("%s='%s'" % (k,v))
		else:
			exec("%s=%d" % (k,int(v)))
						
	if i!=-1:	
		try:	
			return eval(condi)
		except:	
			conn.send("Syntax error : wrong where clause\n")
			return 'break'  
	else:
		return True
		
def print_headers(tok,conn):
	reply = ''
	for col in tok['columns']:
		reply = reply + col + "\t|\t"
	reply =reply + "\n"
	conn.sendall(reply)				

def clientthread(conn):
    flag=1
    conn.send('Welcome to the server.\n') 
    conn.send('Enter the query(semicolon is not requiered):\n') 
    while True:
    	f = open(filepath,"rb")
        cr = csv.reader(f)
        header = cr.next()
        data = conn.recv(1024)
        
        if data=='break':
        	break
        tok = parse.test(data)
        if type(tok)!=dict:
        	conn.send("Syntax error : "+str(tok)+"\n")
        	f.close()
	elif tok['tables'][0]!=filename:
		conn.send("wrong table\n")
		f.close()
	else:
		i = data.find('where')
		condi = data[(i+6):]		
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
				
				print_headers(tok,conn)
				
				for row in cr:
					c = check(row,i,condi,conn)
       					if c==True:
						reply = ''
						for col in tok['columns']:
							reply = reply + row[col] + "\t|\t"
						reply =reply + "\n"
						conn.send(reply)
					elif c=='break':
						break;	
				f.close()
			flag = 1
		else:
			f.close()
			f = open(filepath,"rb")
			cr = csv.DictReader(f)
			
			print_headers(tok)
			
			for row in cr:
				c = check(row,i,condi,conn)
       				if c==True:
					reply = ''
					for col in row:
						reply = reply + row[col] + "\t|\t"
					reply =reply + "\n"
					conn.send(reply)
				elif c=='break':
					break;	
					
			f.close()	
					
    conn.close()
  
 
HOST = ''  
PORT = int(input("Enter the port no.-"))
filename = str(input('Enter the file name(eg.:- \'actors\')\n'))
filepath = str(input('Enter the file path(eg.:- \'\home\Desktop\actors.csv\')\n')) 
 
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



