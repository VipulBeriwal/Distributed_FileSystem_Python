#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import socket
import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

import Server_Services
import file_handle
from file_handle import file_handle_creator
import shelve



#CREATION OF FILE_HANDLE_TABLE
#creating hashtable of custom datatype
fh_table = defaultdict(file_handle_creator)
#list of path to pass in corrupt module
variable = shelve.open("corrupt.dat")
corrupt_block_list = []
counter = 0
#shelve variable dic takes string
#taking permanent storage for corrupting data
link_table = {}
mountname = ''

class Memory(LoggingMixIn, Operations):
    
    def __init__(self, meta_port, Dataserver0port, Dataserver1port, Dataserver2port, Dataserver3port):
        #we have to pass the values in int
        self.service = Server_Services.Server(int(meta_port), int(Dataserver0port), int(Dataserver1port), int(Dataserver2port), int(Dataserver3port))
        object_fh = self.service.fh_Pull("metaserver", '/', '0')
        fh_table['/'] = object_fh
        print("************************************INIT FUNCTION IS EXCUTED COMPLETELY***************************")
        self.s = "My name is vipul beriwal. I study in University of Florida"
        '''print("Convert to string is called")
        time.sleep(2)
        self.service.write_on_server("/ABC", convert_to_list(self.s))
        print("Write Completed")
        time.sleep(5)
        print("Reading the value of path /ABC.....")
        time.sleep(10)
        print(self.service.read_from_server("/ABC"))
        time.sleep(10)'''
        #print(fh_table['/'].path)
    def hardlink(self, path):
        global link_table
        if path in link_table:
            return link_table[path]
        else:
            return path

    def getattr(self, path, fh=None):
        print("|| THE GETATTR FUNCTION IS CALLED ||")
        print("The value of path this time is ", path)
        if path in fh_table:
            #as fh is already there, so asking metadata
            print("========================================================================Path for getattr in memeory.py", path)
            #if we are passing file handle it must be present inht ilookup of metaserver
            metadata = self.service.fh_Push("metaserver", fh_table[path], '0')
            print(metadata)
        else:
            print("The path is not in fh_table")
            raise FuseOSError(ENOENT)
            '''#asking fh for the first time
            object_fh = self.service.fh_Pull("metaserver", path)
            fh_table[path] = object_fh
            #now fh exists in table, it askes for metadata
            metadata = self.service.fh_Push("metaserver", fh_table[path])'''
            #WHY WHY WHY WHYW WHWYW WHWYWW? WHY it is necessary to returnmetadta
        return metadata


    def mkdir(self, path, mode):
        #will give the path and no value to return
        #will call differnt function 
        #once the directory is made in server we also have to update the fh table bcoz it is the direct that was asked!
        #pulling the filehandle from the server
        print("MKDIR IS CALLED")
        object_fh = self.service.fh_Pull("metaserver", path, '1')
        #now this path will exist in server
        fh_table[path] = object_fh
        print("THE VALUE OF FH AFTER RETURN")
        print("Checking the correctness of file handle")
        print(fh_table[path].path)
        #updating the fh_table

    def readdir(self, path, fh):
        content_list = self.service.fh_Push("metaserver", fh_table[path], '1')
        return (['.', '..'] + content_list)

    def create(self, path, mode):
        #it only create the file, data is written by the write function.
        #it treturns the fd.
        print("Create is called")
        time.sleep(0)
        object_fh = self.service.fh_Pull("metaserver", path, '2')
        fh_table[path] = object_fh
        print(fh_table[path].path)
        #we have to write naull empty file in the server's 
        #making dummy update to upload the file in dataservers
        self.service.null_file_write(path)
        print("Null file write sucess")
        time.sleep(0)
        #to save paths in corrupt dictionaries
        global variable
        global corrupt_block_list
        global counter
        corrupt_block_list.insert(counter, path) 
        variable["0"] = corrupt_block_list
        counter += 1
        print(variable["0"])
        time.sleep(0)
        variable.sync()
        return fh_table[path].file_descriptor

    def open(self, path, flags):
        object_fh = self.service.fh_Pull("metaserver", path, '3')
        #saving updated file handle as we incremented file desciptor in server
        fh_table[path] = object_fh
        return fh_table[path].file_descriptor

    def write(self, path, data, offset, fh):
        #first fetch the metadata
        metadata = self.service.fh_Push("metaserver", fh_table[path], '0')
        #copying the size of file
        print(metadata)
        offset = metadata['st_size']
        print("Write functionality from serverservices is being called")
        time.sleep(0)
        #now for hardlink update
        path = self.hardlink(path)
        new_data_length = self.service.write_functionality(path, offset, data, "write")
        print("The length of new data", new_data_length)
        return new_data_length
    
    def read(self, path, size, offset, fh):
        print("read of memory.py is called from server is called")
        time.sleep(0)
        #for hardlink functionality
        path = self.hardlink(path)
        print(path)
        value = self.service.read_from_server(path)
        #we have to read full file
        #to read full file
        print("The value of string is ", value)
        print("the offset is  ", offset)
        time.sleep(0)
        return value[offset:offset + size]

    def rename(self, old, new):
        #rename calls gttr mkdir as path=new [if dir->dir move command given or calls create file->file move command is given 
        #metadata id fetch in old object
        metadata = self.service.rename(old, new)
        print("rename is completed###################")
        print(metadata)
        time.sleep(0)
        #updating fh_table by asking new file handle because properties like abs patha nd all have chnaged
        fh = self.service.fh_Pull("metaserver", new, '0')
        print("new fh received")
        fh_table[new] = fh
        del fh_table[old]
        #dataserver rename has to be done
        if metadata['st_mode'] == (S_IFREG | 0755):
            self.service.rename_dataservers(old, new)
            #updating corrupt file also 
            global variable
            global corrupt_block_list
            if old in corrupt_block_list:
    	        index = corrupt_block_list.index(old)
    	        corrupt_block_list[index] = new
    	        variable['0'] = corrupt_block_list
    	        variable.sync()
        
    def link(self, target, source):
        print("Link function is called")
        value = self.service.link(source, target)
        #updating fh_table
        handle = fh_table[source]
        handle.path = target
        fh_table[target] = handle
        #mapping number of hardlinks to one path
        global link_table
        link_table[target] = source

    def rmdir(self, path):
        value = self.service.remove(path, 'dir')
        if value == True:
            del fh_table[path]

    def unlink(self, path):
        ack1 = self.service.remove(path, "file")
        if ack1 == True:
		if path in fh_table:
        		ack2 = self.service.delete_data(path)
            		del fh_table[path]
            	global link_table
            	if path in link_table:
                	del link_table[path]
            		global corrupt_block_list
            	if path in corrupt_block_list:
	            	corrupt_block_list.pop(corrupt_block_list.index(path))
	            	variable["0"] = corrupt_block_list
	            	variable.sync()

    def symlink(self, target, source):
        print(target)
        print(source)
        print("symlink is called")
        value = self.service.symlink(source, target, mountname)
        object_fh = self.service.fh_Pull("metaserver", target, '2')
        global fh_table
        fh_table[target] = object_fh

    def truncate(self, path, length, fh=None):
        #checking for hardlinkpath)
        path = self.hardlink(path)
        print(length)
        #reading data
        data = self.service.read_from_server(path)
        data = data[:length]
        #changing offset
        offset = 0
        new_data_length = self.service.write_functionality(path, offset, data, "truncate")
        #it will update the metadata in the function 
        return 0


if __name__ == '__main__':
    if len(argv) != 7:
        print('usage: <mountpoint> port_number Dataserver0portnumber Dataserver1portnumber Dataserver2portnumber Dataserver3portnumber')
        exit(1)
    global mountname
    mountname = argv[1]
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(argv[2], argv[3], argv[4], argv[5], argv[6]), argv[1], foreground=True, debug=True)
