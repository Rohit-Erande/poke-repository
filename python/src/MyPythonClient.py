import socket
import time
import comm_pb2
import struct



choice = int(raw_input("Enter your choice: 1. SignUp , 2. Login,  3. List the Courses, 4. Enroll , 5.Course Description 6.View my Course 7. Competition: "))
print choice

if choice == 1:
  fName = raw_input("Enter your First Name: ")
  lName = raw_input("Enter your Last Name: ")    
  username = raw_input("Enter Username :")
  password = raw_input("Enter Password :")
  print " Username is :"+username+" Password is : "+password
elif choice == 2:
  username = raw_input("Enter Username :")
  password = raw_input("Enter Password :")
  print " Username is :"+username+" Password is : "+password 
elif choice == 3:
  print "We offer following courses : "
elif choice == 4:
  course_number = raw_input("Enter the course number you want to enroll to :")
  username = raw_input("Enter Username :")
  print "Enrolled successfully!"
elif choice == 5:
  course_number = raw_input("Enter the course number you want to view the description to :")
  print "Course description for the course ID "+course_number+" is as follows : "
elif choice == 6:
  username = raw_input("Enter Username :")
  print "You are enrolled into following courses!"
else:
  print "Starting competition amongst Leaders!" 

def get_message(sock, msgtype):
    """ Read a message from a socket. msgtype is a subclass of
        of protobuf Message.
    """
    print "inside get message"
    len_buf = socket_read_n(sock, 4)
    msg_len = struct.unpack('>L', len_buf)[0]
    msg_buf = socket_read_n(sock, msg_len)

    msg = msgtype()
    msg.ParseFromString(msg_buf)
      
    return msg


def socket_read_n(sock, n):
    """ Read exactly n bytes from the socket.
        Raise RuntimeError if the connection closed before
        n bytes were read.
    """

    buf = ''
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf


def ping_message(sock):
    request=comm_pb2.Request()
    request.header.routing_id=request.header.PING
    request.header.originator="Client"
    request.header.tag="test finger"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.ping.number = 1234
    request.body.ping.tag = "test"	    	
    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send ping data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)
    return	

def signup(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS     
    request.header.originator="Client"
    request.header.tag="signup"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="1"
    request.body.job_op.data.name_space="Sign_Up"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="1"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN   

    request.body.job_op.data.options.node_type=request.body.job_op.data.options.NODE

    x=request.body.job_op.data.options.node.add()
    x.node_type=request.body.job_op.data.options.VALUE
    x.name="fName"    
    x.value=fName      
    
    x1=request.body.job_op.data.options.node.add()
    x1.node_type=request.body.job_op.data.options.VALUE
    x1.name="lName"    
    x1.value=lName      
    
    x2=request.body.job_op.data.options.node.add()
    x2.node_type=request.body.job_op.data.options.VALUE
    x2.name="username"    
    x2.value=username      
    
    x3=request.body.job_op.data.options.node.add()
    x3.node_type=request.body.job_op.data.options.VALUE
    x3.name="password"    
    x3.value=password         
       
    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send enroll data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)         
    return    

def login(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS 	
    request.header.originator="Client"
    request.header.tag="login"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="2"
    request.body.job_op.data.name_space="login"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="2"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN   

    request.body.job_op.data.options.node_type=request.body.job_op.data.options.NODE

    x=request.body.job_op.data.options.node.add()
    x.node_type=request.body.job_op.data.options.VALUE
    x.name="username"    
    x.value=username      
    
    x1=request.body.job_op.data.options.node.add()
    x1.node_type=request.body.job_op.data.options.VALUE
    x1.name="password"    
    x1.value=password     

    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send some data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest) 		
    return	

def listcourse(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS     
    request.header.originator="Client"
    request.header.tag="listcourse"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="3"
    request.body.job_op.data.name_space="list"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="3"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN
    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send listcourse data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)         
    return     

def enroll(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS     
    request.header.originator="Client"
    request.header.tag="enroll"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="4"
    request.body.job_op.data.name_space="enroll"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="4"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN   

    request.body.job_op.data.options.node_type=request.body.job_op.data.options.NODE

    x=request.body.job_op.data.options.node.add()
    x.node_type=request.body.job_op.data.options.VALUE
    x.name="username"    
    x.value=username     
    
    x1=request.body.job_op.data.options.node.add()
    x1.node_type=request.body.job_op.data.options.VALUE
    x1.name="course_id"    
    x1.value=course_number      
       
    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send enroll data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)         
    return    

def viewDescription(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS     
    request.header.originator="Client"
    request.header.tag="viewDescription"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="5"
    request.body.job_op.data.name_space="Course_Description"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="5"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN
    
    request.body.job_op.data.options.node_type=request.body.job_op.data.options.NODE

    course=request.body.job_op.data.options.node.add()
    course.node_type=request.body.job_op.data.options.VALUE
    course.name="Course_Id"    
    course.value=course_number 
   

    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send viewDescription data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)         
    return     

def enrolledCourses(sock):
    request=comm_pb2.Request() 
    request.header.routing_id=request.header.JOBS     
    request.header.originator="Client"
    request.header.tag="enrolledCourses"
    request.header.time=long(time.time())
    request.header.toNode = "zero"
    request.body.job_op.action=request.body.job_op.ADDJOB
    request.body.job_op.job_id="6"
    request.body.job_op.data.name_space="enrolled_courses"
    request.body.job_op.data.owner_id=5
    request.body.job_op.data.job_id="6"
    request.body.job_op.data.status=request.body.job_op.data.JOBUNKNOWN
    
    request.body.job_op.data.options.node_type=request.body.job_op.data.options.NODE

    x1=request.body.job_op.data.options.node.add()
    x1.node_type=request.body.job_op.data.options.VALUE
    x1.name="Username"    
    x1.value=username 
        
    pingRequest = request.SerializeToString()
    packed_len=struct.pack('>L',len(pingRequest))
    print "lets send enrolledCourses data"
    sock.connect((HOST,PORT))
    sock.sendall(packed_len + pingRequest)         
    return    

def namespaceOp(sock):
    print "Inside namespace "
    request = comm_pb2.Request()
    request.header.routing_id = request.header.NAMESPACES     
    request.header.originator = "Client"
    request.header.tag = "competition"
    request.header.time = long(time.time())
    request.header.toNode = "zero"
    request.body.space_op.action = request.body.space_op.ADDSPACE
    request.body.space_op.ns_id = 7
    request.body.space_op.data.ns_id = 2
    request.body.space_op.data.name = "View Description"
    request.body.space_op.data.desc = "competition"
    request.body.space_op.data.created = 4
    
    request.body.space_op.data.last_modified = 3
    request.body.space_op.data.owner = "5"
    
    pingRequest = request.SerializeToString()
    packed_len = struct.pack('>L', len(pingRequest))
    print "lets send viewDescription data"
    sock.connect((HOST, PORT))
    sock.sendall(packed_len + pingRequest)         
    return       


HOST="localhost"
PORT=5573
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#ping_message(s)
#print get_message(s,comm_pb2.Request)
#login(s)

if choice == 1:
    signup(s)
    print get_message(s,comm_pb2.Request)
elif choice == 2:
    login(s)
    print get_message(s,comm_pb2.Request)
elif choice == 3:
    print "you called list courses" 	
    listcourse(s)
    print get_message(s,comm_pb2.Request)
elif choice == 4:
    enroll(s)
    print get_message(s,comm_pb2.Request)
elif choice == 5:
    viewDescription(s)
    print get_message(s,comm_pb2.Request) 
elif choice == 6:
    enrolledCourses(s)
    print get_message(s,comm_pb2.Request) 
else:
    namespaceOp(s)
    print get_message(s,comm_pb2.Request)
print "Thank you for visiting MOOC Tutorial"    
s.close()













