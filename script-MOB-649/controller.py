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
TRACE_FILE = sys.argv[2]
RESTART = False
if len(sys.argv)>3:
    if sys.argv[3] == "True":
        RESTART = True


WINDOW_SIZE = 1

NONE_PORT = 60001
RECONFIGURATOR = "52.26.182.238"
ETHERPAD_FOLDER = "PaxosApp/"

LOG_PROPERTIES = ETHERPAD_FOLDER+"logging.properties"
GP_PROPERTIES = ETHERPAD_FOLDER+"gigapaxos.properties"
LOG4J_PROPERTIES = ETHERPAD_FOLDER+"log4j.properties"
JAR = ETHERPAD_FOLDER+"jar/etherpad.jar"
JVMFLAGS="-ea -Djava.util.logging.config.file="+LOG_PROPERTIES+" -DgigapaxosConfig="+GP_PROPERTIES+" -Dlog4j.configuration="+LOG4J_PROPERTIES

SSL_OPTIONS=' -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/keyStore/node100.jks'

COMMAND = "java "+JVMFLAGS+SSL_OPTIONS+" -cp "+JAR+" "


### These constant are used for matlab only

M_COMMAND = "reconfiguration"
M_FILE = M_COMMAND + ".m"


TEMPLATE1 = '''
plot(x, y, '-.', x, b,'-', 'LineWidth',2);
legend('Moving average latency', 'Application latency');
xlabel('Request');
ylabel('Moving average of response latency(ms)');
'''

TEMPLATE2 = '''
set(gca, 'FontName', 'Times New Roman');
set(gca, 'FontSize', 24);
grid on;

saveas(gcf,'reconfiguration_etherpad.pdf','pdf');
'''



result = []
cv = Condition()
finished = False

def loadHost():
    dic = {}
    fin = open("ec2-servers","r")
    for line in fin:
        line = line[:-1]
        line = line.split(" ")
        dic[line[0]] = (line[1], line[2])
    #print dic
    return dic

hostToName = loadHost()


class MyTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(1024).strip()
        
        print datetime.datetime.now(),"receive data:",self.data,"from ",self.request
        global result
        result.append(self.data)
        global finished
        finished = True
        print "try to acquire lock"
        cv.acquire()
        print "acquired lock"
        cv.notify()
        cv.release()
        print "release lock"


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
            chan = ssh.get_transport().open_session()
            chan.exec_command(self.cmd)
            ssh.close()
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise


# This function can be changed in the future, if the model to
# start the client is changed
def runClient(host, num_req):
    cmd = COMMAND+"etherpad.ReconfigurableEtherpadExpClient "
    cmd += str(num_req)
    #cmd += hostToName[host][0]
    cmd += " true > output &"
    print cmd
    th = cmdThread(host, cmd)
    th.start()

def runClientRMC(host, num_req):
    print "Start sending "+str(num_req)+" request to "+host
    cmd = "./jdk1.8.0_92/bin/"+COMMAND+"etherpad.ReconfigurableEtherpadExpClient "
    cmd += str(num_req)
    cmd = "ssh -i id_rsa umass_nameservice@"+host+" "+cmd
    print cmd
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = p.communicate()
    if err is None:
        print out
        flag = False
        global result
        lines = out.split("\n")
        line = None
        for l in lines:
            if "RESPONSE:" in l:
                line = l.replace("RESPONSE:", "")
                flag = True
        if flag:
            result.append(line)           
        else:
            print "This run fails",out
            return False
    else:
        print err
        return False
    return True

# Do not use script, hard code the command!
def stopHost(host):
    cmd = "./PaxosApp/clear.sh "
    #print cmd
    th = cmdThread(host, cmd)
    th.start()

def startHost(host):
    cmd = COMMAND + "edu.umass.cs.reconfiguration.ReconfigurableNode " + hostToName[host][1]+" &"
    #print cmd
    th = cmdThread(host, cmd)
    th.start()

def sendRequests(trace):
    for load in trace:
        host = load[0]
        num_req = load[1]
        sent = runClientRMC(host, num_req)
        if not sent:
            print "Experiment failed"
            break
        print "this round is done for host "+host
    print "Send all requests!"


def loadTrace():
    arr = []
    fin = open(TRACE_FILE,"r")
    for line in fin:
        line = line[:-1]
        line = line.split(" ")
        arr.append((line[0], int(line[1])))
    return arr

def avg(arr):
    return sum(arr)/len(arr)

def movingAverage(l):
    arr = []
    for i in range(len(l)):
        if i-WINDOW_SIZE < 0:
            begin = 0
        else:
            begin = i-WINDOW_SIZE
        history = avg(l[begin:i+1])
        #history = item*alpha+history*(1-alpha)
        arr.append(history)
    return arr

def smooth(l):
    arr = []
    arr = l[2:]
    arr = arr[:2]+arr
    return arr

def processResult():
    yaxis = []
    for item in result:
        latencies = item.split(",")
        latencies = map(int, latencies)
        latencies = smooth(latencies)
        latencies = movingAverage(latencies)
        yaxis = yaxis + latencies
        
    #xaxis = range(len(yaxis))
    #print "xaxis:",xaxis

    print "yaxis:",yaxis
    return yaxis
        
def restartServers():
    for host in hostToName.keys():
        stopHost(host)
    stopHost(RECONFIGURATOR)
    print "Stop servers ... done!"
    time.sleep(5)
    
    for host in hostToName.keys():
        startHost(host)
    th = cmdThread(RECONFIGURATOR, COMMAND+"edu.umass.cs.reconfiguration.ReconfigurableNode 900 &")
    th.start()
    time.sleep(5)
    print "Start servers ... done!"
    
    th = cmdThread(RECONFIGURATOR, COMMAND+"etherpad.ReconfigurableEtherpadAppClient &")
    th.start()
    time.sleep(2)
    print "Create group ... done!"
    raw_input()

def runPlot(y):
    # get the maximal number
    m = max(y)
    m += 100
    x = len(y)+1
    #axis([0 40 0 1000]);
    figureSize = "axis([0 "+str(x)+" 0 "+str(m)+"]);\n"
    firstLine = "y = "+str(y)+";\n"+"x="+str(range(len(y)))+";\n"
    benchMark = "b = ones("+str(len(y))+",1);\n"+"b=b*41;\n"
    fout = open(M_FILE, "w+")
    fout.write(benchMark)
    fout.write(firstLine)
    fout.write(TEMPLATE1)
    fout.write(figureSize)
    fout.write(TEMPLATE2)
    fout.close()

    os.system("matlab -nodesktop -r "+M_COMMAND)
    
def main():
    # Step 0: prepare
    #serverThread().start()
    
    # Step 1:
    if RESTART:        
        print 'Restart all servers ...'
        restartServers()
    
    # Step 2: load trace
    print 'Load trace ...'
    trace = loadTrace()
    
    # Step 3: send requests
    print 'Start sending requests according to the trace ...'
    sendRequests(trace)

    # Step 4: plot data
    print 'Process the result ...'
    y = processResult()

    # Step 5: output .m file and plot figure
    runPlot(y)

if __name__ == "__main__":
    main()
