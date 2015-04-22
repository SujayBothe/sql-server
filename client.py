import socket               

s = socket.socket()         
host = socket.gethostname()
port = 8888               

s.connect((host, port))

print s.recv(1024)
temp = input()
s.send(temp)

print s.recv(1024)
temp = input()
s.send(temp)

print s.recv(1024)

while True:	
	temp = input()
	s.send(temp)
	if temp=='break':
		break;
	temp = s.recv(1024)
	print temp 	
s.close                    


