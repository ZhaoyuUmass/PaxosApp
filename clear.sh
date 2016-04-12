#!/bin/bash
rm -r paxos_logs/ reconfiguration_DB derby.log
./ec2Server.sh stop all
echo ""
