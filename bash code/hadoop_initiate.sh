#!/bin/bash
sudo service ssh start
ssh localhost
hadoop/hadoop-3.3.0/sbin/start-dfs.sh
hadoop/hadoop-3.3.0/sbin/start-yarn.sh
