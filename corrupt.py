import Server_Services 
from sys import argv, exit
from xmlrpclib import Binary, ServerProxy
import xmlrpclib
import random
import socket
import logging, pickle, os
from pickle import dumps, loads
import time
import os
import shelve



class corrupt():
	def __init__(self, Dataserver0port, Dataserver1port, Dataserver2port, Dataserver3port):
		self.dataserver0 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver0port)), allow_none=True)
		self.dataserver1 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver1port)), allow_none=True)
		self.dataserver2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver2port)), allow_none=True)
		self.dataserver3 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver3port)), allow_none=True)
		print(self.dataserver0)
		#corrupted list is fetched
		variable = shelve.open("corrupt.dat", 'r')
		variable.sync()
		self.corrupted_list = variable["0"] 
		print(self.corrupted_list)
		
		if self.corrupted_list == []:
			print("there is no data in the servers. Please first write in the server")
			time.sleep(2)
			exit(1)
		
		self.path = random.choice(self.corrupted_list)
		#object to call functions of server_services
		#to call write function of server services
		self.OBJECT = Server_Services.Server(0000, Dataserver0port, Dataserver1port, Dataserver2port, Dataserver3port)

	def read(self):
		print("This read is to fetch data blocks to corrupt files")
		#initialized dictionaries
		d0 = {}
		d1 = {}
		d2 = {}
		d3 = {}
		#it will tell from where the respective dictionaries
		d0_servno = 0
		d1_servno = 0
		d2_servno = 0
		d3_servno = 0

		# put exception handling of data servers here
		#for server 0
		ack = False
		while(ack != True):
		    try:
		    	ack = pickle.loads(self.dataserver0.active().data)
		    	#ack = pickle.loads(self.dataserver0.active().data)
		        #asking data from the dataserver0
		        print("Fetching data block from dataserver0.............")
	    
		        time.sleep(2)
		        d0 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(self.path)), Binary('0')).data)
		        print(d0)
		        print("Success")
		        d0_servno = 0
		        break
		    except socket.error:
		        #failed to get data from dataserver 0
		        print("dataserver0 connection failed !")
		        time.sleep(2)
		        print("Now Contacting dataserver1.................")
		        time.sleep(2)
		        #trying to connect the dataserver1
		        try:
		            ack = pickle.loads(self.dataserver1.active().data)
		            print("Fetching data block from dataserver1............")
		            time.sleep(2)
		            d0 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(self.path)), Binary('0')).data)
		            print("Success")
		            d0_servno = 1
		            #failed to connect to datserver1 
		            break
		        except socket.error:
		            print("dataserver1 connection failed !")
		            time.sleep(2)
		            print("Now Contacting dataserver2.................")
		            time.sleep(2)
		            try:
		                ack = pickle.loads(self.dataserver2.active().data)
		                print("Fetching data block from dataserver2............")
		                time.sleep(2)
		                d0 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(self.path)), Binary('0')).data)
		                print("Success")
		                d0_servno = 2
		                break
		            except socket.error:
		                print("Connection to redundant dataservers failed...!!!")
		                print("Trying to contact the main server(0) for data again")
		                time.sleep(2)

		#for server 1
		ack = False
		while(ack != True):
		    try:
		        ack = pickle.loads(self.dataserver1.active().data)
		        #asking data from the dataserver1
		        print("Fetching data block from dataserver1.............")
		        time.sleep(2)
		        #the other argument willl reflect from which dataserver the value has to be taken
		        d1 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(self.path)), Binary('1')).data)
		        print("Success")
		        d1_servno = 1
		        break
		    except socket.error:
		        #failed to get data from dataserver 1
		        print("dataserver1 connection failed !")
		        time.sleep(2)
		        print("Now Contacting dataserver2.................")
		        time.sleep(2)
		        #trying to connect the dataserver2
		        try:
		            ack = pickle.loads(self.dataserver2.active().data)
		            print("Fetching data block from dataserver2............")
		            time.sleep(2)
		            d1 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(self.path)), Binary('1')).data)
		            print("Success")
		            d1_servno = 2
		            #failed to connect to datserver2 
		            break
		        except socket.error:
		            print("dataserver2 connection failed !")
		            time.sleep(2)
		            print("Now Contacting dataserver3.................")
		            time.sleep(2)
		            try:
		                ack = pickle.loads(self.dataserver3.active().data)
		                print("Fetching data block from dataserver3............")
		                time.sleep(2)
		                d1 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(self.path)), Binary('1')).data)
		                print("Success")
		                d1_servno = 3
		                break
		            except socket.error:
		                print("Connection to redundant dataservers failed...!!!")
		                print("Trying to contact the main server(1) for data again")
		                time.sleep(2)  
		
		#for server 2
		ack = False
		while(ack != True):
		    try:
		        ack = pickle.loads(self.dataserver2.active().data)
		        #asking data from the dataserver2
		        print("Fetching data block from dataserver2............")
		        time.sleep(2)
		        d2 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(self.path)), Binary('2')).data)
		        print("Success")
		        d2_servno = 2
		        break
		    except socket.error:
		        #failed to get data from dataserver 2
		        print("dataserver2 connection failed !")
		        time.sleep(2)
		        print("Now Contacting dataserver3.................")
		        time.sleep(2)
		        #trying to connect the dataserver3
		        try:
		            ack = pickle.loads(self.dataserver3.active().data)
		            print("Fetching data block from dataserver3.............")
		            time.sleep(2)
		            d2 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(self.path)), Binary('2')).data)
		            print("Success")
		            d2_servno = 3
		            #failed to connect to datserver3
		            break
		        except socket.error:
		            print("dataserver3 connection failed !")
		            time.sleep(2)
		            print("Now Contacting dataserver0.................")
		            time.sleep(2)
		            try:
		                ack = pickle.loads(self.dataserver0.active().data)
		                print("Fetching data block from dataserver0...............")
		                time.sleep(2)
		                d2 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(self.path)), Binary('2')).data)
		                print("Success")
		                d2_servno = 0
		                break
		            except socket.error:
		                print("Connection to redundant dataservers failed...!!!")
		                print("Trying to contact the main server(2) for data again")
		                time.sleep(2)
		#for server 3

		ack = False
		while(ack != True):
		    try:
		        ack = pickle.loads(self.dataserver3.active().data)
		        #asking data from the dataserver3
		        print("Fetching data block from dataserver3.............")
		        time.sleep(2)
		        d3 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(self.path)), Binary('3')).data)
		        print("Success")
		        d3_servno = 3  
		        break
		    except socket.error:
		        #failed to get data from dataserver 3
		        print("dataserver3 connection failed !")
		        time.sleep(2)
		        print("Now Contacting dataserver0.................")
		        time.sleep(2)
		        #trying to connect the dataserver0
		        try:
		            ack = pickle.loads(self.dataserver0.active().data)
		            print("Fetching data block from dataserver0.............")
		            time.sleep(2)
		            d3 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(self.path)), Binary('3')).data)
		            print("Success")
		            d3_servno = 0 
		            #failed to connect to datserver0
		            break
		        except socket.error:
		            print("dataserver0 connection failed !")
		            time.sleep(2)
		            print("Now Contacting dataserver1.................")
		            time.sleep(2)
		            try:
		                ack = pickle.loads(self.dataserver1.active().data)
		                print("Fetching data block from dataserver1.............")
		                time.sleep(2)
		                d3 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(self.path)), Binary('3')).data)
		                print("Success")
		                d3_servno = 1
		                break
		            except socket.error:
		                print("Connection to redundant dataservers failed...!!!")
		                print("Trying to contact the main server(3) for data again")
		                time.sleep(2)
        
		#initializing list to choose random servers
		L0 = []
		L1 = []
		L2 = []
		L3 = []
		
		#storing keys in list
		print("Extracting keys form dictionaries for block numbers")
		for x in d0.keys():
			L0.append(x)
		for x in d1.keys():
			L1.append(x)
		for x in d2.keys():
			L2.append(x)
		for x in d3.keys():
			L3.append(x)
		print(L0, L1, L2, L3)
		time.sleep(1)

		#checking for null entry
		if L0 != []:
			r0 = random.choice(L0)
			#corrupting values
			d0[r0] = d0[r0][:8]
			d0[r0] = d0[r0][:1] + "$" + d0[r0][2:5] + "$" + d0[r0][6:8]
			print(d0[r0])
		else:
			r0 = None
		
		if L1 != []:
			r1 = random.choice(L1)
			#corrupting values
			d1[r1] = d1[r1][:8]
			d1[r1] = d1[r1][:1] + "$" + d1[r1][2:5] + "$" + d1[r1][6:8]
			print(d1[r1])
		else:
			r1 = None
		
		if L2 != []:
			r2 = random.choice(L2)
			#corrupting values
			d2[r2] = d2[r2][:8]
			d2[r2] = d2[r2][:1] + "$" + d2[r2][2:5] + "$" + d2[r2][6:8]
			print(d2[r2])
		else:
			r2 = None

		if L3 != []:
			r3 = random.choice(L3)
			#corrupting values
			d3[r3] = d3[r3][:8]
			d3[r3] = d3[r3][:1] + "$" + d3[r3][2:5] + "$" + d3[r3][6:8]
			print(d3[r3])
		else:
			r3 = None
		time.sleep(2)
		print("If block number does not exist then prints the false with correct value otherwise prints corrupted value with block number")
		print("The random block number from d0 and its value is " + str(r0))
		print("The random block number from d1 and its value is " + str(r1))
		print("The random block number from d2 and its value is " + str(r2))
		print("The random block number from d3 and its value is " + str(r3))
		time.sleep(2)
		print("Corrupted data block written succesfully [main server] - [preserving copy of another server]")
		#writing corrupted data on the servers
		if r0 != None:
			#write_block(path, corrupted data, block_no, "main server to be contacted", "value of copy in the main server"):
			x = random.choice([0,1,2])
			if x == 0:
				if self.OBJECT.write_block(self.path, d0[r0], r0, "0", "0"):
					print("Corrupted data block written succesfully 0-0")
			elif x == 1:
				if self.OBJECT.write_block(self.path, d0[r0], r0, "1", "0"):
					print("Corrupted data block written succesfully 1-0")
			elif x == 2:
				if self.OBJECT.write_block(self.path, d0[r0], r0, "2", "0"):
					print("Corrupted data block written succesfully 2-0")


		if r1 != None:
			#write_block(path, corrupted data, block_no, "main server to be contacted", "value of copy in the main server"):
			x = random.choice([1,2,3])
			if x == 1:
				if self.OBJECT.write_block(self.path, d1[r1], r1, "1", "1"):
					print("Corrupted data block written succesfully 1-1")
			elif x == 2:
				if self.OBJECT.write_block(self.path, d1[r1], r1, "2", "1"):
					print("Corrupted data block written succesfully 2-1")
			elif x == 3:
				if self.OBJECT.write_block(self.path, d1[r1], r1, "3", "1"):
					print("Corrupted data block written succesfully 3-1")

		if r2 != None:
			#write_block(path, corrupted data, block_no, "main server to be contacted", "value of copy in the main server"):
			x = random.choice([2,3,0])
			if x == 2:
				if self.OBJECT.write_block(self.path, d2[r2], r2, "2", "2"):
					print("Corrupted data block written succesfully 2-2")
			elif x == 3:
				if self.OBJECT.write_block(self.path, d2[r2], r2, "3", "2"):
					print("Corrupted data block written succesfully 3-2")
			elif x == 0:
				if self.OBJECT.write_block(self.path, d2[r2], r2, "0", "2"):
					print("Corrupted data block written succesfully 0-2")

		if r3 != None:
			#write_block(path, corrupted data, block_no, "main server to be contacted", "value of copy in the main server"):
			x = random.choice([3,0,1])
			if x == 3:
				if self.OBJECT.write_block(self.path, d3[r3], r3, "3", "3"):
					print("Corrupted data block written succesfully 3-3")
			elif x == 0:
				if self.OBJECT.write_block(self.path, d3[r3], r3, "0", "3"):
					print("Corrupted data block written succesfully 0-3")
			elif x == 1:
				if self.OBJECT.write_block(self.path, d3[r3], r3, "1", "3"):
					print("Corrupted data block written succesfully 1-3")

			
		if(r0 == r1 == r2 == r3 == None):
			print("No data in the server to be corrupted")

		time.sleep(2)

if __name__ == '__main__':
    if len(argv) != 5:
        print('	usage: <corrupt.py> Dataserver0portnumber Dataserver1portnumber Dataserver2portnumber Dataserver3portnumber')
        exit(1)
    else:
    	for i in range (0, 6):
    		print("")
    		print("***************************VIRUS ACTIVATED******************************")
    		time.sleep(0.5)
    		os.system('clear')
    		time.sleep(0.5)

    	print("Random block of a self.path in each dataserver is corrupting..........")
    	time.sleep(1)
    	dataserver = corrupt(argv[1], argv[2], argv[3], argv[4])
    	dataserver.read()
