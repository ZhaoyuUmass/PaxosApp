#!/bin/bash
HEAD=`dirname $0`
rm -r paxos_logs/ reconfiguration_DB derby.log
./$HEAD/ec2Server.sh stop all
echo ""
