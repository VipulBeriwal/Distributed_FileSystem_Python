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
import shelve
import time



# Presents a HT interface
class SimpleHT:
  #lookup for file handle
  def __call__(self):
    print("hello")

  def active(self):
    return Binary(pickle.dumps(True))

  def get(self, path, server_table):
    path = pickle.loads(path.data)
    global datafile3
    global datafile1
    global datafile2
    if server_table == '3':
      return Binary(pickle.dumps((datafile3[path])))
    elif server_table == '1':
      print("The value of dataserver1 is taken from dataserver3")
      time.sleep(1)
      return Binary(pickle.dumps((datafile1[path])))
    elif server_table == '2':
      print("The value of dataserver2 is taken from dataserver3")
      time.sleep(1)
      return Binary(pickle.dumps((datafile2[path])))

  def put(self, path, server_table, index, value):
    global variable
    global datafile3
    global datafile1
    global datafile2
    path = path.data
    server_table = server_table.data
    index = pickle.loads(index.data)
    value = pickle.loads(value.data)
    #round robin
    if server_table == "3":
      datafile3[path][index] = value
      variable['3'] = datafile3
      variable.sync()
      return Binary(pickle.dumps(True))
    elif server_table == "1":
      datafile1[path][index] = value
      variable['1'] = datafile1
      variable.sync()
      return Binary(pickle.dumps(True))
    elif server_table == "2":
      datafile2[path][index] = value
      variable['2'] = datafile2
      variable.sync()
      return Binary(pickle.dumps(True))
    elif server_table == "3-3-NULL":
      datafile3[path] = {}
      variable['3'] = datafile3
      variable.sync()
      return Binary(pickle.dumps(True))
    elif server_table == "3-1-NULL":
      datafile1[path] = {}
      variable['1'] = datafile1
      variable.sync()
      return Binary(pickle.dumps(True))
    elif server_table == "3-2-NULL":
      datafile2[path] == {}
      variable['2'] = datafile2
      variable.sync()
      return Binary(pickle.dumps(True))

  def one_block(self, path, block_value, server_no):
    block_value = pickle.loads(block_value.data)
    path = pickle.loads(path.data)
    server_no = pickle.loads(server_no.data)
    if server_no == '3':
      return Binary(pickle.dumps(datafile3[path][block_value]))
    elif server_no == '1':
      return Binary(pickle.dumps(datafile1[path][block_value]))
    elif server_no == '2':
      return Binary(pickle.dumps(datafile2[path][block_value])) 

  def rename(self, old, new):
    old = pickle.loads(old.data)
    new = pickle.loads(new.data)
    
    datafile3[new] = datafile3[old]
    del datafile3[old]
    variable['3'] = datafile3
    variable.sync()
    
    datafile1[new] = datafile1[old]
    del datafile1[old]
    variable['1'] = datafile1
    variable.sync()
    
    datafile2[new] = datafile2[old]
    del datafile2[old]
    variable['2'] = datafile2
    variable.sync()
    return Binary(pickle.dumps(True))


  def delete(self, path):
    path = pickle.loads(path.data)
    del datafile1[path]
    variable['1'] = datafile1
    variable.sync()
    del datafile3[path]
    variable['3'] = datafile3
    variable.sync()
    del datafile2[path]
    variable['2'] = datafile2
    variable.sync()
    return Binary(pickle.dumps(True))

def main(port_number):
  print("***************************************dataserver3 is running***********************************************", int(port_number))
  #mount data initilaization
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  #print(args)
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = port_number
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
  file_server.register_function(sht.active)
  file_server.register_function(sht.put)
  file_server.register_function(sht.one_block)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.delete)
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

    time.sleep(1)
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
  if len(argv) != 5:
    print("usage : python datserver0.py <portnumber> dataserver1.py <portnumber> dataserver2.py <portnumber> dataserver3.py <portnumber>")
    exit(1)

  else:
    #define global variable port and it will be updated 
    port = 0000

    variable = shelve.open("dataserver3.dat")

    if bool(variable) == False:
      print("For the first time it is running")
      time.sleep(0)
      #defining dictionary for data storage
      datafile3 = defaultdict(dict)
      #maintaining copy of server 2
      datafile1 = defaultdict(dict)
      #maintaining copy of datserver3
      datafile2 = defaultdict(dict)
      #saving in the shelve
      variable['3'] = datafile3
      variable['1'] = datafile1
      variable['2'] = datafile2

    datafile3 = variable['3']
    datafile1 = variable['1']
    datafile2 = variable['2']
    print(datafile3)
    print(datafile1)
    print(datafile2)
    time.sleep(0)
    main(argv[4])
