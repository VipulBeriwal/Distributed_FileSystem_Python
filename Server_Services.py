from __future__ import print_function, absolute_import, division
#server modules and packages
import xmlrpclib
from xmlrpclib import Binary, ServerProxy
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging, pickle, os
from pickle import dumps, loads
#operational packages
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import time
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from file_handle import file_handle_creator
import socket
from String import convert_to_list
import hashlib
import random
import md5
#for error purpose


class Server():
    def __init__(self, meta_port, Dataserver0port, Dataserver1port, Dataserver2port, Dataserver3port):
        self.metaserver = xmlrpclib.ServerProxy("http://localhost:"+str(int(meta_port)), allow_none=True)
        self.dataserver0 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver0port)), allow_none=True)
        self.dataserver1 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver1port)), allow_none=True)
        self.dataserver2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver2port)), allow_none=True)
        self.dataserver3 = xmlrpclib.ServerProxy("http://localhost:"+str(int(Dataserver3port)), allow_none=True)

    def link(self, path, hardlink_path):
        return pickle.loads((self.metaserver.update_metadata_link(Binary(pickle.dumps(path)), Binary(pickle.dumps(hardlink_path)))).data)

    def remove(self, path, op):
        return pickle.loads((self.metaserver.remove_dir(Binary(pickle.dumps(path)), Binary(pickle.dumps(op)))).data)

    def symlink(self, target, source, mountname):
        return pickle.loads((self.metaserver.symlink(Binary(pickle.dumps(target)), Binary(pickle.dumps(source)), Binary(pickle.dumps(mountname)))).data)

     
    def delete_data(self, path):
        ack0 = self.check_active(self.dataserver0)
        ack1 = self.check_active(self.dataserver1)
        ack2 = self.check_active(self.dataserver2)
        ack3 = self.check_active(self.dataserver3)
        if ack0 == ack1 == ack2 == ack3 == True:
            print("All dataservers are active")
            ack0 = pickle.loads((self.dataserver0.delete(Binary(pickle.dumps(path)))).data)
            ack1 = pickle.loads((self.dataserver1.delete(Binary(pickle.dumps(path)))).data)
            ack2 = pickle.loads((self.dataserver2.delete(Binary(pickle.dumps(path)))).data)
            ack3 = pickle.loads((self.dataserver3.delete(Binary(pickle.dumps(path)))).data)
            if ack0 == ack1 == ack2 == ack3 == True:
                return True
  
    def fh_Pull(self, s, path, op):
        #returns file handle 
        if s == "metaserver":
            return pickle.loads((self.metaserver.get(Binary(path), Binary(op))).data)

    def fh_Push(self, s, fh, op):
        #returns metadata
        if s == "metaserver":
            #dumping  the file handle and then converting into binary
            #data will comeinto binary form, therefore accessed first as .data and then loaded!
            return pickle.loads((self.metaserver.put(Binary(pickle.dumps(fh)), Binary(op))).data)

    def rename(self, old, new):
        return pickle.loads((self.metaserver.rename(Binary(pickle.dumps(old)), Binary(pickle.dumps(new)))).data)
    
    def read_from_server(self, path):
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
                #asking data from the dataserver0
                print("Fetching data block from dataserver0.............")
                time.sleep(0)
                d0 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(path)), Binary('0')).data)
                print(d0)
                print("Success")
                d0_servno = 0
                break
            except socket.error:
                #failed to get data from dataserver 0
                print("dataserver0 connection failed !")
                time.sleep(0)
                print("Now Contacting dataserver1.................")
                time.sleep(0)
                #trying to connect the dataserver1
                try:
                    ack = pickle.loads(self.dataserver1.active().data)
                    print("Fetching data block from dataserver1............")
                    time.sleep(0)
                    d0 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(path)), Binary('0')).data)
                    print("Success")
                    d0_servno = 1
                    #failed to connect to datserver1 
                    break
                except socket.error:
                    print("dataserver1 connection failed !")
                    time.sleep(0)
                    print("Now Contacting dataserver2.................")
                    time.sleep(0)
                    try:
                        ack = pickle.loads(self.dataserver2.active().data)
                        print("Fetching data block from dataserver2............")
                        time.sleep(0)
                        d0 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(path)), Binary('0')).data)
                        print("Success")
                        d0_servno = 2
                        break
                    except socket.error:
                        print("Connection to redundant dataservers failed...!!!")
                        print("Trying to contact the main server(0) for data again")
                        time.sleep(0)


        #for server 1
        ack = False
        while(ack != True):
            try:
                ack = pickle.loads(self.dataserver1.active().data)
                #asking data from the dataserver1
                print("Fetching data block from dataserver1.............")
                time.sleep(0)
                #the other argument willl reflect from which dataserver the value has to be taken
                d1 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(path)), Binary('1')).data)
                print("Success")
                d1_servno = 1
                break
            except socket.error:
                #failed to get data from dataserver 1
                print("dataserver1 connection failed !")
                time.sleep(0)
                print("Now Contacting dataserver2.................")
                time.sleep(0)
                #trying to connect the dataserver2
                try:
                    ack = pickle.loads(self.dataserver2.active().data)
                    print("Fetching data block from dataserver2............")
                    time.sleep(0)
                    d1 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(path)), Binary('1')).data)
                    print("Success")
                    d1_servno = 2
                    #failed to connect to datserver2 
                    break
                except socket.error:
                    print("dataserver2 connection failed !")
                    time.sleep(0)
                    print("Now Contacting dataserver3.................")
                    time.sleep(0)
                    try:
                        ack = pickle.loads(self.dataserver3.active().data)
                        print("Fetching data block from dataserver3............")
                        time.sleep(0)
                        d1 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(path)), Binary('1')).data)
                        print("Success")
                        d1_servno = 3
                        break
                    except socket.error:
                        print("Connection to redundant dataservers failed...!!!")
                        print("Trying to contact the main server(1) for data again")
                        time.sleep(0)
        
        
        #for server 2
        ack = False
        while(ack != True):
            try:
                ack = pickle.loads(self.dataserver2.active().data)
                #asking data from the dataserver2
                print("Fetching data block from dataserver2............")
                time.sleep(0)
                d2 = pickle.loads(self.dataserver2.get(Binary(pickle.dumps(path)), Binary('2')).data)
                print("Success")
                d2_servno = 2
                break
            except socket.error:
                #failed to get data from dataserver 2
                print("dataserver2 connection failed !")
                time.sleep(0)
                print("Now Contacting dataserver3.................")
                time.sleep(0)
                #trying to connect the dataserver3
                try:
                    ack = pickle.loads(self.dataserver3.active().data)
                    print("Fetching data block from dataserver3.............")
                    time.sleep(0)
                    d2 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(path)), Binary('2')).data)
                    print("Success")
                    d2_servno = 3
                    #failed to connect to datserver3
                    break
                except socket.error:
                    print("dataserver3 connection failed !")
                    time.sleep(0)
                    print("Now Contacting dataserver0.................")
                    time.sleep(0)
                    try:
                        ack = pickle.loads(self.dataserver0.active().data)
                        print("Fetching data block from dataserver0...............")
                        time.sleep(0)
                        d2 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(path)), Binary('2')).data)
                        print("Success")
                        d2_servno = 0
                        break
                    except socket.error:
                        print("Connection to redundant dataservers failed...!!!")
                        print("Trying to contact the main server(2) for data again")
                        time.sleep(0)
        #for server 3

        ack = False
        while(ack != True):
            try:
                ack = pickle.loads(self.dataserver3.active().data)
                #asking data from the dataserver3
                print("Fetching data block from dataserver3.............")
                time.sleep(0)
                d3 = pickle.loads(self.dataserver3.get(Binary(pickle.dumps(path)), Binary('3')).data)
                print("Success")
                d3_servno = 3  
                break
            except socket.error:
                #failed to get data from dataserver 3
                print("dataserver3 connection failed !")
                time.sleep(0)
                print("Now Contacting dataserver0.................")
                time.sleep(0)
                #trying to connect the dataserver0
                try:
                    ack = pickle.loads(self.dataserver0.active().data)
                    print("Fetching data block from dataserver0.............")
                    time.sleep(0)
                    d3 = pickle.loads(self.dataserver0.get(Binary(pickle.dumps(path)), Binary('3')).data)
                    print("Success")
                    d3_servno = 0 
                    #failed to connect to datserver0
                    break
                except socket.error:
                    print("dataserver0 connection failed !")
                    time.sleep(0)
                    print("Now Contacting dataserver1.................")
                    time.sleep(0)
                    try:
                        ack = pickle.loads(self.dataserver1.active().data)
                        print("Fetching data block from dataserver1.............")
                        time.sleep(0)
                        d3 = pickle.loads(self.dataserver1.get(Binary(pickle.dumps(path)), Binary('3')).data)
                        print("Success")
                        d3_servno = 1
                        break
                    except socket.error:
                        print("Connection to redundant dataservers failed...!!!")
                        print("Trying to contact the main server(3) for data again")
                        time.sleep(0)

        #to track if first server is the key then what is the contacted server from whihc data came
        server_tracker = {'0': d0_servno, '1': d1_servno, '2': d2_servno, '3': d3_servno}

        #chksum logic
        #comnbining dictionaries
        combined_dic = d0.copy()
        combined_dic.update(d1)
        combined_dic.update(d2)
        combined_dic.update(d3)

        data_for_checksum = {}
        recieved_checksum = {}
        for i in combined_dic:
            data_for_checksum[i] = combined_dic[i][:8]
            recieved_checksum[i] = combined_dic[i][8:]
        print(data_for_checksum)
        print(recieved_checksum)

        #passing data dict for new checksum, list is passed empty
        new_checksum = self.checksum([], 1, data_for_checksum)

        for i in recieved_checksum:
            if new_checksum[i] == recieved_checksum[i]:
                print("Checksum matches for the block ", i)
                time.sleep(0)
            else:
                print("Checksum of the block " + str(i) + " fails")
                #original server from whihc the block should have returned
                original_server_of_block = self.original_server(i)
                print("Original server of block", original_server_of_block)
                #dictionary to map the s_servno
                servno = server_tracker[original_server_of_block]
                #contacted server from where the block is returned due to server shutdown
                contacted_server_of_block = self.fetched_server(original_server_of_block, servno)
                print("Contacted server of block", contacted_server_of_block)
                time.sleep(0)
                #randomly asks for other server to read the data
                to_contact = self.select_random(original_server_of_block, contacted_server_of_block)
                print("The server from which we have to contact for the replacement", to_contact)
                #returns value in int form and we have to pass in string form
                print(str(to_contact))
                print("Block number", i)
                correct_block = self.read_block(path, i, original_server_of_block, str(to_contact), "checksum")
                print(correct_block)
                time.sleep(0)

                if correct_block == False:
                    #coming out of the function
                    return "Invalid data"
                elif correct_block == None:
                    exit(1)
                else:
                    #updating the block in the server
                    print("The write_block is called for update")
                    time.sleep(0)
                    '''write block is used by corrupt and this function. Arguments of write_block relatively defined as-
                    Corrupt uses => 
                    original server = main server that is to be contacted   
                    server_number = the datafile number in that server whose data to be corrupted
                    Read (this function)uses it as - 
                    original server = datafile server number
                    server_number = main server that is to be connected
                    Swapping arguments for read function to call write block
                    instead of giving(original_server_of_block, contacted_server_of_block) we give
                    contacted_server_of_block, original_server_of_block'''

                    if self.write_block(path, correct_block, i, contacted_server_of_block, original_server_of_block):
                        print("The corrupted data block has been replaced")
                        time.sleep(0)
                    #updating the related dictionaries
                    if original_server_of_block == '0':
                        d0[i] = correct_block
                    elif original_server_of_block == '1':
                        d1[i] = correct_block
                    elif original_server_of_block == '2':
                        d2[i] = correct_block
                    elif original_server_of_block == '3':
                        d3[i] = correct_block
                print("All operations regarding the checksum has been made, you will get the correct data now")
        

        #combining roundrobin
        '''first we start infinite loop. We used exceptional handling to check if the key is not present when everytime we increment 
         the value of i. it will throw error for the key and that block will not be present in the data server.
         now we are getting dictionaries regarding the path values given and combining them round robinlt to get the entire string'''
        #initialization for the loop
        i = 0
        string = ''
        print("8 byte data integration will start")
        while(i > -1):
            if (i%4 == 0):
                try:
                    string = string + d0[i][:8]
                except KeyError:
                    break
            elif (i%4 == 1):
                try:
                    string = string + d1[i][:8]
                except KeyError:
                    break
            elif (i%4 == 2):
                try:
                    string = string + d2[i][:8]
                except KeyError:
                    break
            elif (i%4 == 3):
                try:
                    string = string + d3[i][:8]
                except KeyError:
                    break
            i += 1
            #incrementing the values each time
            time.sleep(0)
            print(string)
        return string

    #check active functin implementation
    def check_active(self, server_object):
        try:
            value = pickle.loads(server_object.active().data)
        except socket.error:
            value = False
        return value  

    def write_functionality(self, path, offset, data, op):
        #offset is the old length of data
        block_no = int(offset/8)
        server_no = int(block_no % 4)
        print("The block number where the update has to start is ", block_no)
        print("The server_no in which the update has to be started", server_no)
        time.sleep(0)
        
        if offset != 0:
            print("Read block function is called")
            block_data = self.read_block(path, block_no, server_no, "", "write")
            print("The value of block data", block_data)
            time.sleep(0)
            #removinf null values
            for i in range (0, len(block_data)):
                if block_data[i] == "\0":
                    block_data = block_data[:i]
                    break
            print(block_data)
            time.sleep(0)
            new_data = block_data[:-1] + data
            #for removing \n  
            #-1 beacuse it adds /n aotumatically in the end, reads /n as only 1 character
            list_data = convert_to_list(new_data)
            print("New list has been made when offset is not zero")
            print(list_data)
            time.sleep(0)
        else:
            if op == "write":
                new_data = data
            else:
                new_data = data
            #-1 is not included beacuse we have to get linux line cursor in next line
            list_data = convert_to_list(new_data)
            print("New list has been made when offset is zero", list_data)
            time.sleep(0)

        print("Writing data on the servers")
        self.write_on_server(path, block_no, list_data)
        
        #updating metadata
        print("Updating metadata....")
        time.sleep(0)
        #The length of data in metadata will be old length + new length
        updated_length = offset + len(data)
        print("The length of new data to be written in metaserver is ",updated_length)
        value = pickle.loads((self.metaserver.update_metadata(Binary(pickle.dumps(path)), Binary(pickle.dumps(updated_length)))).data)
        if value == True:
            print("Metaserver update has been done")
            time.sleep(0)

        #returning length of new data
        return updated_length

    def null_file_write(self, path):
        print("As we create metadata during create the dataservers must be updated with file path and with null values in data")
        value = False
        while(value != True):
            w0 = self.check_active(self.dataserver0)
            w1 = self.check_active(self.dataserver1)
            w2 = self.check_active(self.dataserver2)
            w3 = self.check_active(self.dataserver3)
            if(w0 == w1 == w2 == w3 == True):
                print("All dataservers are active")
                z00 = pickle.loads((self.dataserver0.put(Binary(path), Binary("0-0-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z02 = pickle.loads((self.dataserver0.put(Binary(path), Binary("0-2-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z03 = pickle.loads((self.dataserver0.put(Binary(path), Binary("0-3-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                print("Null write successfull for dataserver0")
                z11 = pickle.loads((self.dataserver1.put(Binary(path), Binary("1-1-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z10 = pickle.loads((self.dataserver1.put(Binary(path), Binary("1-0-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z13 = pickle.loads((self.dataserver1.put(Binary(path), Binary("1-3-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                print("Null write successfull for dataserver1")
                z22 = pickle.loads((self.dataserver2.put(Binary(path), Binary("2-2-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z20 = pickle.loads((self.dataserver2.put(Binary(path), Binary("2-0-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z21 = pickle.loads((self.dataserver2.put(Binary(path), Binary("2-1-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                print("Null write successfull for dataserver2")
                z33 = pickle.loads((self.dataserver3.put(Binary(path), Binary("3-3-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z31 = pickle.loads((self.dataserver3.put(Binary(path), Binary("3-1-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                z32 = pickle.loads((self.dataserver3.put(Binary(path), Binary("3-2-NULL"), Binary(pickle.dumps("none")), Binary(pickle.dumps("none")))).data)
                print("Null write successfull for dataserver3")
                if (z00 == z02 == z03 == z11 == z10 == z13 == z22 == z20 == z21 == z33 == z31 == z32 == True):
                    print("Success in null-write in all servers")
                    value = True
                    time.sleep(0)
                else:
                    print("Contacting servers again..............")
                    time.sleep(0)

    def write_on_server(self, path, block_no, list_data):
        #checksum
        list_data = self.checksum(list_data, 0, {})
        for i in range (0, len(list_data)):
            #it has to be started from the given server numbers
            if (i+block_no)%4 == 0:
                a = 0
                while (a < 2):
                    ack0 = self.check_active(self.dataserver0)
                    ack1 = self.check_active(self.dataserver1)
                    ack2 = self.check_active(self.dataserver2)
                    if (ack0 and ack1 and ack2) == True:
                        print("All required dataservers are active. Updating value to dataserver0, dataserver1, dataserver2..............")
                        #we passed 3 values dataserver, path, which server table
                        r0 = pickle.loads(self.dataserver0.put(Binary(path), Binary("0"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r1 = pickle.loads(self.dataserver1.put(Binary(path), Binary("0"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r2 = pickle.loads(self.dataserver2.put(Binary(path), Binary("0"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        time.sleep(0)
                        if (r0 and r1 and r2) == True:
                            print("Success in write for dataserver0")
                            print("Success in write for dataserver1")
                            print("Success in write for dataserver2")
                            print("Value [" + str(list_data[i]) + "] to all servers has been written and acknowledged successfully")
                            time.sleep(0)
                            a = 2
                        else: 
                            print(" Trying agian to contact servers................ ")
                    elif ack0 == False:
                        print("Connection to dataserver0 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack1 == False:
                        print("Connection to dataserver1 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack2 == False:
                        print("Connection to dataserver2 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
            elif (i+block_no)%4 == 1:
                a = 0
                while (a < 2):
                    ack1 = self.check_active(self.dataserver1)
                    ack2 = self.check_active(self.dataserver2)
                    ack3 = self.check_active(self.dataserver3)
                    if (ack1 and ack2 and ack3) == True:
                        print("All required dataservers are active. Updating value to dataserver1, dataserver2, dataserver3..............")
                        #we passed 3 values dataserver, path, which server table
                        r1 = pickle.loads(self.dataserver1.put(Binary(path), Binary("1"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r2 = pickle.loads(self.dataserver2.put(Binary(path), Binary("1"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r3 = pickle.loads(self.dataserver3.put(Binary(path), Binary("1"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        time.sleep(0)
                        if (r1 and r2 and r3) == True:
                            print("Success in write for dataserver1")
                            print("Success in write for dataserver2")
                            print("Success in write for dataserver3")
                            print("Value [" + str(list_data[i]) + "] to all servers has been written and acknowledged successfully")
                            time.sleep(0)
                            a = 2
                        else: 
                            print(" Trying agian to contact servers................ ")
                    elif ack1 == False:
                        print("Connection to dataserver1 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack2 == False:
                        print("Connection to dataserver2 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack3 == False:
                        print("Connection to dataserver3 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)

            elif (i+block_no)%4 == 2:
                a = 0
                while (a < 2):
                    ack2 = self.check_active(self.dataserver2)
                    ack3 = self.check_active(self.dataserver3)
                    ack0 = self.check_active(self.dataserver0)
                    if (ack2 and ack3 and ack0) == True:
                        print("All required dataservers are active. Updating value to dataserver2, dataserver3, dataserver0..............")
                        #we passed 3 values dataserver, path, which server table
                        r2 = pickle.loads(self.dataserver2.put(Binary(path), Binary("2"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r3 = pickle.loads(self.dataserver3.put(Binary(path), Binary("2"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r0 = pickle.loads(self.dataserver0.put(Binary(path), Binary("2"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        time.sleep(0)
                        if (r2 and r3 and r0) == True:
                            print("Success in write for dataserver2")
                            print("Success in write for dataserver3")
                            print("Success in write for dataserver0")
                            print("Value [" + str(list_data[i]) + "] to all servers has been written and acknowledged successfully")
                            time.sleep(0)
                            a = 2
                        else: 
                            print(" Trying agian to contact servers................ ")
                    elif ack2 == False:
                        print("Connection to dataserver2 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack3 == False:
                        print("Connection to dataserver3 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack0 == False:
                        print("Connection to dataserver0 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)

            elif (i+block_no)%4 == 3:
                a = 0
                while (a < 2):
                    ack3 = self.check_active(self.dataserver3)
                    ack0 = self.check_active(self.dataserver0)
                    ack1 = self.check_active(self.dataserver1)
                    if (ack3 and ack0 and ack1) == True:
                        print("All required dataservers are active. Updating value to dataserver3, dataserver0, dataserver1..............")
                        #we passed 3 values dataserver, path, which server table
                        r3 = pickle.loads(self.dataserver3.put(Binary(path), Binary("3"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r0 = pickle.loads(self.dataserver0.put(Binary(path), Binary("3"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        r1 = pickle.loads(self.dataserver1.put(Binary(path), Binary("3"), Binary(pickle.dumps(i+block_no)), Binary(pickle.dumps(list_data[i]))).data)
                        time.sleep(0)
                        if (r3 and r0 and r1) == True:
                            print("Success in write for dataserver3")
                            print("Success in write for dataserver0")
                            print("Success in write for dataserver1")
                            print("Value [" + str(list_data[i]) + "] to all servers has been written and acknowledged successfully")
                            time.sleep(0)
                            a = 2
                        else: 
                            print(" Trying agian to contact servers................ ")
                    elif ack3 == False:
                        print("Connection to dataserver3 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack0 == False:
                        print("Connection to dataserver0 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
                    elif ack1 == False:
                        print("Connection to dataserver1 is failed!")
                        print("Trying again to connect.......")
                        time.sleep(0)
        print(" *************************Write Success***********************************")
        time.sleep(0) 
  
    def read_block(self, path, block_no, server_number, to_contact, op):
        if op == "write":
            print("Fetching the the block of the data......")
            if server_number == 0:
                ack = False
                while(ack != True):
                    print("Contacting the server0 for the block......")
                    w0 = self.check_active(self.dataserver0)
                    if w0 == False:
                        print("Contacting server1 for the block....... ")
                        w1 = self.check_active(self.dataserver1)
                        if w1 == False:
                            print("Contacting server2 for the block....... ")
                            w2 = self.check_active(self.dataserver2)
                            if w2 == False:
                                print("Connect to all datservers failed")
                                print("contacting again")
                            else:
                                print("Success")
                                time.sleep(0)
                                ack = True
                                temp = pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                                return temp[:8]
                        else:
                            print("Success")
                            time.sleep(0)
                            ack = True
                            temp = pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                            return temp[:8]

                    else:
                        print("Success")
                        time.sleep(0)
                        ack = True
                        temp = pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                        return temp[:8]

            elif server_number == 1:
                ack = False
                while(ack != True):
                    print("Contacting the server1 for the block......")
                    w1 = self.check_active(self.dataserver1)
                    if w1 == False:
                        print("Contacting server2 for the block....... ")
                        w2 = self.check_active(self.dataserver2)
                        if w2 == False:
                            print("Contacting server3 for the block....... ")
                            w3 = self.check_active(self.dataserver3)
                            if w3 == False:
                                print("Connect to all datservers failed")
                                print("contacting again")
                            else:
                                print("Success")
                                time.sleep(0)
                                ack = True
                                temp = pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                                return temp[:8]
                        else:
                            print("Success")
                            time.sleep(0)
                            ack = True
                            temp = pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                            return temp[:8]

                    else:
                        print("Success")
                        time.sleep(0)
                        ack = True
                        temp = pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                        return temp[:8]

            elif server_number == 2:
                ack = False
                while(ack != True):
                    print("Contacting the server2 for the block......")
                    w2 = self.check_active(self.dataserver2)
                    if w2 == False:
                        print("Contacting server3 for the block....... ")
                        w3 = self.check_active(self.dataserver3)
                        if w3 == False:
                            print("Contacting server0 for the block....... ")
                            w0 = self.check_active(self.dataserver0)
                            if w0 == False:
                                print("Connect to all datservers failed")
                                print("contacting again")
                            else:
                                print("Success")
                                time.sleep(0)
                                ack = True
                                temp = pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                                return temp[:8]
                        else:
                            print("Success")
                            time.sleep(0)
                            ack = True
                            temp = pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                            return temp[:8]

                    else:
                        print("Success")
                        time.sleep(0)
                        ack = True
                        temp = pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                        return temp[:8]

            elif server_number == 3:
                ack = False
                while(ack != True):
                    print("Contacting the server3 for the block......")
                    w3 = self.check_active(self.dataserver3)
                    if w3 == False:
                        print("Contacting server0 for the block....... ")
                        w0 = self.check_active(self.dataserver0)
                        if w0 == False:
                            print("Contacting server1 for the block....... ")
                            w1 = self.check_active(self.dataserver1)
                            if w1 == False:
                                print("Connect to all datservers failed")
                                print("contacting again")
                            else:
                                print("Success")
                                time.sleep(0)
                                ack = True
                                temp = pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                                return temp[:8]
                        else:
                            print("Success")
                            time.sleep(0)
                            ack = True
                            temp = pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                            return temp[:8]

                    else:
                        print("Success")
                        time.sleep(0)
                        ack = True
                        temp = pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                        return temp[:8]

        elif op == "checksum":
            print("Make all servers active to fetch the correct data")
            time.sleep(30)
            if server_number == '0':
                if to_contact == '1':
                    k1 = self.check_active(self.dataserver1)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver1 and main server was datserver0")
                        time.sleep(0)
                        return pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                    else:
                        return False
                elif to_contact == '2':
                    k1 = self.check_active(self.dataserver2)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver2 and main server was datserver0")
                        time.sleep(0)
                        return pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                    else:
                        return False
                elif to_contact == '0':
                    k1 = self.check_active(self.dataserver0)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver0 and main server was datserver0")
                        time.sleep(0)
                        return pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('0')))).data)
                    else:
                        return False
            elif server_number == '1':
                if to_contact == '1':
                    k1 = self.check_active(self.dataserver1)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver1 and main server was datserver1")
                        time.sleep(0)
                        return pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                    else:
                        return False
                elif to_contact == '2':
                    k1 = self.check_active(self.dataserver2)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver12and main server was datserver1")
                        time.sleep(0)
                        return pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                    else:
                        return False
                elif to_contact == '3':
                    k1 = self.check_active(self.dataserver3)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver3 and main server was datserver1")
                        time.sleep(0)
                        return pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('1')))).data)
                    else:
                        return False
            elif server_number == '2':
                if to_contact == '2':
                    k1 = self.check_active(self.dataserver2)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver2 and main server was datserver2")
                        time.sleep(0)
                        return pickle.loads((self.dataserver2.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                    else:
                        return False
                elif to_contact == '3':
                    k1 = self.check_active(self.dataserver3)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver3 and main server was datserver2")
                        time.sleep(0)
                        return pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                    else:
                        return False
                elif to_contact == '0':
                    k1 = self.check_active(self.dataserver0)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver0 and main server was datserver2")
                        time.sleep(0)
                        return pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('2')))).data)
                    else:
                        return False
            elif server_number == '3':
                if to_contact == '3':
                    k1 = self.check_active(self.dataserver3)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver3 and main server was datserver3")
                        time.sleep(0)
                        return pickle.loads((self.dataserver3.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                    else:
                        return False
                elif to_contact == '0':
                    k1 = self.check_active(self.dataserver0)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver0 and main server was datserver3")
                        time.sleep(0)
                        return pickle.loads((self.dataserver0.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                    else:
                        return False
                elif to_contact == '1':
                    k1 = self.check_active(self.dataserver1)
                    if k1 == True:
                        print("Checksum datablock is fetched from dataserver1 and main server was datserver3")
                        time.sleep(0)
                        return pickle.loads((self.dataserver1.one_block(Binary(pickle.dumps(path)), Binary(pickle.dumps(block_no)), Binary(pickle.dumps('3')))).data)
                    else:
                        return False

    def checksum(self, list_data, op, dic):
        if op == 0:
            for i in range (0, len(list_data)):
                hash_variable = md5.new()
                hash_variable.update(list_data[i])
                list_data[i] = list_data[i] + hash_variable.hexdigest()
            return list_data
        elif op == 1:
            new_chksum_dic = {}
            for i in dic:
                hash_variable = md5.new()
                hash_variable.update(dic[i])
                new_chksum_dic[i] = hash_variable.hexdigest()
            return new_chksum_dic

    def original_server(self, block_no):
        if block_no%4 == 0:
            return '0'
        elif block_no%4 == 1:
            return '1'
        elif block_no%4 == 2:
            return '2'
        elif block_no%4 ==3:
            return '3'

    def fetched_server(self, original_server_of_block, servno):
        if original_server_of_block == '0':
            if servno == 0:
                contacted_server = '0'
            elif servno == 1:
                contacted_server = '1'
            elif servno == 2:
                contacted_server = '2'
            return contacted_server

        elif original_server_of_block == '1':
            if servno == 1:
                contacted_server = '1'
            elif servno == 2:
                contacted_server = '2'
            elif servno == 3:
                contacted_server = '3'
            return contacted_server

        elif original_server_of_block == '2':
            if servno == 2:
                contacted_server = '2'
            elif servno == 3:
                contacted_server = '3'
            elif servno == 0:
                contacted_server = '0'
            return contacted_server

        elif original_server_of_block == '3':
            if servno == 3:
                contacted_server = '3'
            elif servno == 0:
                contacted_server = '0'
            elif servno == 1:
                contacted_server = '1'
            return contacted_server

    def select_random(self, original_server, contacted_server):
        if original_server == '0':
            #contacted server from where the data is wrongly fetched
            if contacted_server == '0':
                return random.choice([1,2])
            elif contacted_server == '1':
                return random.choice([0,2])
            elif contacted_server == '2':
                return random.choice([0,1])

        elif original_server == '1':
            if contacted_server == '1':
                return random.choice([2,3])
            elif contacted_server == '2':
                return random.choice([1,3])
            elif contacted_server == '3':
                return random.choice([1,2])

        elif original_server == '2':
            if contacted_server == '2':
                return random.choice([3,0])
            elif contacted_server == '3':
                return random.choice([2,0])
            elif contacted_server == '0':
                return random.choice([2,3])

        elif original_server == '3':
            if contacted_server == '3':
                return random.choice([1,0])
            elif contacted_server == '0':
                return random.choice([1,3])
            elif contacted_server == '1':
                return random.choice([0,3])

    def write_block(self, path, value, block_no, original_server, server_number):
        ack = 0
        if original_server == '0':
            while(ack < 1):
                z = self.check_active(self.dataserver0)
                if (z == True):
                    print("Updating the correct/wrong value of block to dataserver0")
                    time.sleep(0)
                    if server_number == '0':
                       return pickle.loads((self.dataserver0.put(Binary(path), Binary("0"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '2':
                       return pickle.loads((self.dataserver0.put(Binary(path), Binary("2"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '3':
                       return pickle.loads((self.dataserver0.put(Binary(path), Binary("3"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                else:
                    print("Connection failed during update - write!")
                    print("Waiting for the dataserver0 to be up..........")
                    time.sleep(0)

        elif original_server == '1':
            while(ack < 1):
                z = self.check_active(self.dataserver1)
                if (z == True):
                    print("Updating the correct/wrong value of block to dataserver1")
                    time.sleep(0)
                    if server_number == '1':
                       return pickle.loads((self.dataserver1.put(Binary(path), Binary("1"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '0':
                       return pickle.loads((self.dataserver1.put(Binary(path), Binary("0"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '3':
                       return pickle.loads((self.dataserver1.put(Binary(path), Binary("3"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                else:
                    print("Connection failed during update - write!")
                    print("Waiting for the dataserver1 to be up..........")
                    time.sleep(0)

        elif original_server == '2':
            while(ack < 1):
                z = self.check_active(self.dataserver2)
                if (z == True):
                    print("Updating the correct/wrong value of block to dataserver2")
                    time.sleep(0)
                    if server_number == '2':
                       return pickle.loads((self.dataserver2.put(Binary(path), Binary("2"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '0':
                       return pickle.loads((self.dataserver2.put(Binary(path), Binary("0"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '1':
                       return pickle.loads((self.dataserver2.put(Binary(path), Binary("1"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                else:
                    print("Connection failed during update - write!")
                    print("Waiting for the dataserver2 to be up..........")
                    time.sleep(0)

        elif original_server == '3':
            while(ack < 1):
                z = self.check_active(self.dataserver3)
                if (z == True):
                    print("Updating the correct/wrong value of block to dataserver3")
                    time.sleep(0)
                    if server_number == '3':
                       return pickle.loads((self.dataserver3.put(Binary(path), Binary("3"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '1':
                       return pickle.loads((self.dataserver3.put(Binary(path), Binary("1"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                    elif server_number == '2':
                       return pickle.loads((self.dataserver3.put(Binary(path), Binary("2"), Binary(pickle.dumps(block_no)), Binary(pickle.dumps(value)))).data)
                else:
                    print("Connection failed during update - write!")
                    print("Waiting for the dataserver3 to be up..........")
                    time.sleep(0)

    def rename_dataservers(self, old, new):
        print("Renaming paths in dataservers for rename ")
        time.sleep(0)
        value = False
        while(value != True):
            w0 = self.check_active(self.dataserver0)
            w1 = self.check_active(self.dataserver1)
            w2 = self.check_active(self.dataserver2)
            w3 = self.check_active(self.dataserver3)
            if(w0 == w1 == w2 == w3 == True):
                print("All dataservers are active")
                z0 = pickle.loads((self.dataserver0.rename(Binary(pickle.dumps(old)), Binary(pickle.dumps(new)))).data)
                z1 = pickle.loads((self.dataserver1.rename(Binary(pickle.dumps(old)), Binary(pickle.dumps(new)))).data)
                z2 = pickle.loads((self.dataserver2.rename(Binary(pickle.dumps(old)), Binary(pickle.dumps(new)))).data)
                z3 = pickle.loads((self.dataserver3.rename(Binary(pickle.dumps(old)), Binary(pickle.dumps(new)))).data)
                
                if (z0 == z1 == z2 == z3 == True):
                    print("Success in Renaming")
                    value = True
                    time.sleep(0)
                else:
                    print("Contacting servers again..............")
                    time.sleep(0)




        
            

