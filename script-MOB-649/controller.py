import paramiko
import threading
import SocketServer
import time
import datetime
import subprocess
import sys
import os

from threading import Condition

KEY_FILE = "EC2"
NONE_PORT = 60001

result = []
cv = Condition()

def loadHost():
    dic = {}
    fin = open("ec2-servers","r")
    for line in fin:
        line = line[:-1]
        line = line.split(" ")
        dic[line[0]] = line[1]
    return dic

hostToName = loadHost()


class MyTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(1024).strip()
        
        print datetime.datetime.now(),"receive data:",self.data,"from ",self.request
        global result
        result.append(self.data)
        cv.aquire()
        cv.notify()
        cv.release()


class serverThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        HOST, PORT = "none.cs.umass.edu", NONE_PORT
        print 'Start server at port '+str(PORT)
        server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)
        server.serve_forever()

class cmdThread (threading.Thread):
    def __init__(self, host, cmd):
        threading.Thread.__init__(self)
        self.host = host
        self.cmd = cmd

    def run(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(self.host, username = 'ubuntu', key_filename=KEY_FILE)
            stdin, stdout, stderr = ssh.exec_command(self.cmd)
            ssh.close()
        except:
            print 'Time out'


# This function can be changed in the future, if the model to
# start the client is changed
def runClient(host, num_req):
    cmd = "./PaxosEtherpad/ec2Client.sh etherpad.ReconfigurableEtherpadExpClient "
    cmd += hostToName[host]
    th = cmdThread(host, cmd)
    th.start()


def sendRequests(trace):
    for load in trace:
        host = load[0]
        num_req = load[1]
        runClient(host, num_req)
        finished = False
        cv.aquire()
        while not finished:            
            cv.wait()
        cv.release()
        print "this round is done for host "+host
    print "Send all requests!"


def loadTrace():
    arr = []
    fin = open("trace","r")
    for line in fin:
        line = line[:-1]
        line = line.split(" ")
        arr.append(line)
    return arr

def movingAverage(l):
    return l

def processResult():
    yaxis = []
    for item in result:
        latencies = item.split(",")
        yaxis = yaxis + latencies
    xaxis = range(len(xaxis))
    yaxis = movingAverage(yaxis)
    print xaxis
    print yaxis    
        

def main():
    # Step 0: start server
    serverThread().start()
    
    # Step 1: load trace
    trace = loadTrace()
    print trace
    
    # Step 2: send requests
    sendRequests(trace)

    # Step 3: plot data
    processResult()

if __name__ == "__main__":
    main()
