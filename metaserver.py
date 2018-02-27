#!/usr/bin/env python

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit
from collections import defaultdict
from stat import S_IFDIR, S_IFLNK, S_IFREG
from pickle import dumps, loads

from file_handle import file_handle_creator
import String
from functionality import meta_object, initialization

#define global variable port and it will be updated 
port = 0000
#global variable to generate inode number
count = 0

#CREATION OF FILE_HANDLE_TABLE
#creating hashtable of custom datatype
#fh_table = defaultdict(file_handle_creator)

#definition of class object of metaserver type

# Presents a HT interface
class SimpleHT:
  #lookup for file handle
  def __call__(self):
    print("hello")


  def get(self, path, op):
    path = path.data
    op = op.data
    #this is when server is initialized
    OBJECT = initialization()
    if op == '0':
      #for first initialization
      print("for returning fh ", path)
      filehandle = OBJECT.return_filehandle(path)
    elif op == '1':
      #for mkdir
      print("The value of op is ", op)
      print("The value of path is ", path)
      filehandle = OBJECT.imakedir(path)
    elif op == '2':
      #for create
      print("The value of op is ", op)
      print("The value of path is ", path)
      filehandle = OBJECT.icreate(path)
    elif op == '3':
      print("The value of op is ", op)
      print("The value of path is ", path)
      filehandle = OBJECT.iopen(path)
    return Binary(pickle.dumps((filehandle)))


  #normal lookup
  def put(self, fh, op):
    fh = pickle.loads(fh.data)
    OBJECT = initialization()
    if op == '0':
      #normal opeartion when asking for metadata
      metadata = OBJECT.return_metadata(fh)
      print("the value of metadata from functionality file")
      return Binary(pickle.dumps((metadata)))
    elif op == '1':
      #reading directory
      content_list = OBJECT.ireaddir(fh.path)
      return Binary(pickle.dumps((content_list)))

  def rename(self, old, new):
    old = pickle.loads(old.data)
    new = pickle.loads(new.data)
    OBJECT = initialization()
    metadata = OBJECT.irename(old, new)
    return Binary(pickle.dumps((metadata)))

  def update_metadata(self, path, new_length):
    path = pickle.loads(path.data)
    new_length = pickle.loads(new_length.data)
    OBJECT = initialization()
    OBJECT.iupdate_metadata(path, new_length)
    return Binary(pickle.dumps((True)))

  def update_metadata_link(self, path, hardlink_path):
    path = pickle.loads(path.data)
    hardlink_path = pickle.loads(hardlink_path.data)
    OBJECT = initialization()
    OBJECT.ilink(path, hardlink_path)
    return Binary(pickle.dumps((True)))

  def remove_dir(self, path, op):
    path = pickle.loads(path.data)
    op = pickle.loads(op.data)
    OBJECT = initialization()
    if op == "dir":
      value = OBJECT.irmdir(path)
      return Binary(pickle.dumps((value)))
    elif op == "file":
      value = OBJECT.irm(path)
      return Binary(pickle.dumps((value)))

  def symlink(self, target, source, mountname):
    target = pickle.loads(target.data)
    source = pickle.loads(source.data)
    mountname = pickle.loads(mountname.data)
    OBJECT = initialization()
    value = OBJECT.isymlink(target, source, mountname)
    return Binary(pickle.dumps((value)))




def main(port_number):
  #mount data initilaization
  OBJECT = initialization()
  OBJECT.initialize()
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  print(args)
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = port_number
  print(port)
  if "--port" in ol:
    port = int(ol["--port"])
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', int(port)), allow_none=True)
  file_server.register_introspection_functions()
  sht = SimpleHT()
  #sht.initialize()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.update_metadata)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.update_metadata_link)
  file_server.register_function(sht.remove_dir)
  file_server.register_function(sht.symlink)
  #file_server.register_function(sht.print_content)
  #file_server.register_function(sht.read_file)
  #file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(0)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(port, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(0)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(0)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")






if __name__ == "__main__":
  if len(argv) != 2:
    print("usage : python sample.py --port <portnumber>")
    exit(1)

  main(argv[1])
