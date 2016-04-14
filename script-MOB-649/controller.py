import paramiko
import threading
import SocketServer
import time
import datetime
import subprocess
import sys
import os

from threading import Condition

KEY_FILE = sys.argv[1]
NONE_PORT = 60001
RECONFIGURATOR = "52.26.182.238"

result = []
cv = Condition()

def loadHost():
    dic = {}
    fin = open("ec2-servers","r")
    for line in fin:
        line = line[:-1]
        line = line.split(" ")
        dic[line[0]] = (line[1], line[2])
    print dic
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
            print "Unexpected error:", sys.exc_info()[0]
            raise


# This function can be changed in the future, if the model to
# start the client is changed
def runClient(host, num_req):
    cmd = "./PaxosEtherpad/ec2Client.sh etherpad.ReconfigurableEtherpadExpClient "
    cmd += str(num_req)+" "
    cmd += hostToName[host][0]
    cmd += " true > output &"
    print cmd
    th = cmdThread(host, cmd)
    th.start()

def stopHost(host):
    cmd = "./PaxosEtherpad/clear.sh"
    print cmd
    th = cmdThread(host, cmd)
    th.start()

def startHost(host):
    cmd = "./PaxosEtherpad/ec2Server.sh start "+hostToName[host][1]
    print cmd
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
        
def restartServers():
    for host in hostToName.keys():
        stopHost(host)
    stopHost(RECONFIGURATOR)
    
    time.sleep(1)
    
    for host in hostToName.keys():
        startHost(host)
    th = cmdThread(RECONFIGURATOR, "./PaxosEtherpad/ec2Server.sh start 900")
    th.start()
    time.sleep(1)

def main():
    # Step 0: prepare
    serverThread().start()

    # Step 1: restart all the servers
    restartServers()
    print "Servers all restart ... "
    raw_input()
    
    # Step 2: load trace
    trace = loadTrace()
    print trace
    
    # Step 3: send requests
    sendRequests(trace)

    # Step 4: plot data
    processResult()

if __name__ == "__main__":
    #runClient("54.67.107.203", 1)
    main()
