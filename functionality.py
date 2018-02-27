
#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
from file_handle import file_handle_creator
import String

import logging, xmlrpclib, pickle
import os
import logging
from xmlrpclib import Binary
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from SimpleXMLRPCServer import SimpleXMLRPCServer
from pickle import dumps, loads
from xmlrpclib import Binary, ServerProxy


size_of_block = 8
port = 0

if not hasattr(__builtins__, 'bytes'):
    bytes = str


#global variable to generate inode number
count = 0

#CREATION OF FILE_HANDLE_TABLE
#creating hashtable of custom datatype
fh_table = defaultdict(file_handle_creator)
fd = 0
#definition of class object of metaserver type

class meta_object():

  def __init__(self, path, data, object_type):
    self.absolute_path = path
    self.type = object_type
    self.name = String.name(path)
    self.parent_path = String.parent_path(path)
    self.metadata = {}
    #if it is a file then metadata will be different and it will a string
    if self.type == 'f':
      self.metadata = dict(st_mode=(S_IFREG | 0755), st_nlink=1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
      self.data = str
      #if it is a directory it will be default dictionary whihc stores object and has different metadata
    elif self.type == 'd':
      self.metadata = dict(st_mode=(S_IFDIR | 0o755), st_ctime=time(), st_mtime=time(), st_atime=time(), st_nlink=2)
      self.data = data
      print(self.metadata)
    elif self.type == 's':
      self.metadata = dict(st_mode=(S_IFLNK | 0777), st_nlink=1, st_ctime=time(), st_mtime=time(),st_atime=time())
      self.data = data
    print("INIT executed sucessfully")

#initialization of global variable root
root = meta_object('', defaultdict(meta_object), '')

class initialization():

  def initialize(self):
    global root
    print("initialisation for root directory")
    #root object ha been initialized
    root = meta_object('/', defaultdict(meta_object), 'd')
    #to increase similarity with linux
    print("initialization of file handle")
    #creating file handle creator
    OBJ = file_handle_creator()
    print("tells the python count as global")
    global count 
    OBJ.inode_number = count
    OBJ.path = '/' 
    count += 1
    print("updating fh_table")
    global fh_table
    fh_table['/'] = OBJ
    print(fh_table['/']) 

  def return_metadata(self, fh):
    print("RETURN METADATA IS CALLED")
    global fh_table
    return (self.ilookup(fh.path)).metadata

  def return_filehandle(self, path):
    print("RETURN FILEHANDLE IS CALLED")
    global fh_table
    return fh_table[path]
  
  def ilookup(self, path):
    print("LOOKUP IS CALLED")
    if path == '/':
      return root
    context = root
    path_parts = path.split("/")[1:]
    for name in path_parts:
      if name in context.data:
        file = context.data[name]
        if file.type == 'f':
          return file
        else:
          context = context.data[name]
    return context
   
  def imakedir(self, path):
    print("imakedir is called")
    print(path)
    new_dir = meta_object(path, defaultdict(meta_object), 'd')
    #inreasinf similarity with linux
    print("*******************************************************************")
    print(new_dir)
    print("THE PARENT PATH")
    print(new_dir.parent_path)
    parent = self.ilookup(new_dir.parent_path)
    #storing parent direct in the child
    print("the absoulte_path of parent")
    parent.data[new_dir.name] = new_dir
    parent.metadata['st_nlink'] += 1
    #creating a file handle
    OBJ = file_handle_creator()
    print("tells the python count as global")
    global count 
    OBJ.inode_number = count
    count += 1
    OBJ.path = path 
    print("updating fh_table")
    global fh_table
    fh_table[path] = OBJ
    print(fh_table) 
    #returns the fh
    return fh_table[path]

  def icreate(self, path):
    print("iCreate is called")
    print(path)
    new_file = meta_object(path, '', 'f')
    #inreasinf similarity with linux
    print("The parent path of the file")
    print(new_file.parent_path)
    parent = self.ilookup(new_file.parent_path)
    #storing parent direct in the child
    parent.data[new_file.name] = new_file
    parent.metadata['st_nlink'] += 1
    #creating a file handle
    OBJ = file_handle_creator()
    print("tells the python count as global")
    global count 
    OBJ.inode_number = count
    count += 1
    OBJ.path = path 
    #updating file descriptor in file descriptor
    global fd
    fd += 1
    OBJ.file_descriptor = fd
    print("updating fh_table")
    global fh_table
    fh_table[path] = OBJ
    print(fh_table) 
    #returns the fh
    return fh_table[path]

  def iopen(self, path):
    global fd
    fd += 1
    fh_table[path].file_descriptor = fd
    return fh_table[path]


  def ireaddir(self, path):
    context = self.ilookup(path)
    print("context returned")
    #will pass list
    content_list = [x for x in context.data]
    print("Will return list in functionality file")
    return content_list

  def iupdate_metadata(self, path, new_length):
    file_object = self.ilookup(path)
    file_object.metadata['st_size'] = new_length
    file_object.metadata['st_ctime'] = time()
    file_object.metadata['st_atime'] = time()

  def irename(self, old, new):
    print("The old path is ", old)
    print("The new path is ", new)
    old_object = self.ilookup(old)
    old_object_parent = self.ilookup(old_object.parent_path)
    #deleting entry
    print(old_object.name)
    #new parent of new file
    new_object = self.ilookup(new)
    #lookup will give the context of the object if it is not present
    if (old_object.type == "f") and (new_object.type == 'd') and (old_object_parent == new_object):
      print("Simple rename function is called")
      old_name = old_object.name
      old_object.name = new[1:]
      old_object.absolute_path = old_object.parent_path + new
      old_object_parent = self.ilookup(old_object.parent_path)
      old_object_parent.data[old_object.name] = old_object
      del old_object_parent.data[old_name]
      fh = fh_table[old_name]
      fh.path = old_object.absolute_path
      fh_table[new] = fh
      del fh_table[old_name]
      return old_object.metadata

    #overwrting file both file exist
    if (old_object.type == "f") and (new_object.type == 'd') and (String.name(new) not in fh_table):
        #normal renaming, lookup will return parent directory
        print(new_object.name)
        #new object will be the parent
        old_object.name = String.name(new)
        print(old_object.name)
        old_object.absolute_path = new_object.absolute_path +'/' + old_object.name
        old_object.parent_path = new_object.absolute_path
        new_object.data[old_object.name] = old_object
        new_object.metadata['st_nlink'] += 1
        print("Rename has been done")
        print(new_object.data)
        temp = old_object
        #updating fh table
        fh = fh_table[old]
        fh.path = old_object.absolute_path
        fh_table[new] = fh
        del fh_table[old]
        del old_object_parent.data[old_object.name]
        old_object_parent.metadata['st_nlink'] -= 1
        print("The entry is deleted")
        return old_object.metadata
    elif new_object.type == "d":
        print("###################################################")
        print(new_object.name)
        #parent will the same in directory case
        #name will be as old name as we are moving the directory
        #type will also be same
        #new object is new parent
        old_object.absolute_path = new_object.absolute_path +'/' + old_object.name
        print(old_object.absolute_path)
        old_object.parent_path = new_object.absolute_path
        #moving directories key will be old objetcs name
        new_object.data[old_object.name] = old_object
        new_object.metadata['st_nlink'] += 1
        print("Rename has been done")
        print(new_object.data)
        temp = old_object
        #updating fh table
        fh = fh_table[old]
        fh.path = old_object.absolute_path
        fh_table[new] = fh
        del fh_table[old]
        #deleting entry
        del old_object_parent.data[old_object.name]
        old_object_parent.metadata['st_nlink'] -= 1
        print("The entry is deleted")
        return temp.metadata
    elif old_object.type == "f" and new_object.type == 'f':
        print("Overrite")
        new_object.name = old_object.name
        new_object.absolute_path = old_object.absolute_path
        old_object_parent = self.ilookup(old_object.parent_path)
        del fh_table[old]
        del old_object_parent.data[old_object.name]
        old_object_parent.metadata['st_nlink'] = -1
        print("Completed")
        return new_object.metadata
    

  def ilink(self, path, hardlink_path):
  	#creates hardlink object
  	hardlink_name = String.name(hardlink_path)
  	hardlink_name_parent = self.ilookup(String.parent_path(hardlink_path))
  	file_object = self.ilookup(path)
  	hardlink_name_parent.data[hardlink_name] = file_object
  	hardlink_name_parent.metadata['st_nlink'] += 1
  	#Updating fh table
  	handle = fh_table[path]
  	handle.path = hardlink_path
  	fh_table[hardlink_path] = handle

  def irmdir(self, path):
    OBJ = self.ilookup(path)
    parent_object = self.ilookup(OBJ.parent_path)
    #if OBJ.metadata['st_nlink'] == 2:
    del parent_object.data[OBJ.name]
    parent_object.metadata['st_nlink'] -= 1
    del fh_table[path]
    return True


  def irm(self, path):
  	OBJ = self.ilookup(path)
  	parent_object = self.ilookup(OBJ.parent_path)
  	del parent_object.data[OBJ.name]
  	parent_object.metadata['st_nlink'] -= 1
  	del fh_table[path]
  	return True
  
  def isymlink(self, target, source, mountname):
    	file_system_os_path = os.getcwd() + '/' + mountname
        if file_system_os_path in source:
            source = source.replace(file_system_os_path, '')
    	source_object = self.ilookup(source)
    	parent_source_object = self.ilookup(target)
    	full_os_path = file_system_os_path + source
    	new = meta_object(target, full_os_path, 's')
    	parent_source_object.data[source_object.name] = new
        parent_source_object.metadata['st_nlink'] += 1
        print("symlink successful")
    	return True


      










    
