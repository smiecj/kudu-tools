#!/usr/bin/python
# -*- coding: UTF-8 -*
import sys, json
jsonObj = json.loads('[{"uuid":"087e3a70007448c4a68b666b3445eb33","rpc-addresses":"table_server_host:7050"}]')
for kudu_node in jsonObj: 
    print kudu_node['rpc-addresses']