#!/usr/bin/python
# -*- coding: UTF-8 -*-

""" KuduTableRemoteParser: parse remote replica command result from tablet server, and summary all tablet size and finally get 
    total size of table
"""
import argparse,os,sys,re
sys.path.append('kudu-tools')

from kudu_table_size_tool import KuduTableSizeTool

class KuduTableRemoteParser:
    def __init__(self, table_name, remote_replica_ret_file_path):
        self.table_name = table_name
        self.remote_replica_ret_path = remote_replica_ret_file_path
    
    ## get kudu table size
    def getTableSizeSplitArr(self):
        ### get remote replica result: will include all table size result, need filter by search table name
        matchPattern = "Table name: {}".format(self.table_name)
        table_size_join_str = ""
        remote_replica_ret_file_arr = os.listdir(self.remote_replica_ret_path)
        for current_file_name in remote_replica_ret_file_arr:
            f = open(os.path.join(self.remote_replica_ret_path, current_file_name))
            current_line = f.readline()
            while len(current_line) != 0:
                if re.match(matchPattern, current_line):
                    f.readline()
                    table_size_ret = f.readline()
                    table_size_ret_split_arr = table_size_ret.split(" ")
                    table_size_str = table_size_ret_split_arr[len(table_size_ret_split_arr) - 1].strip()

                    if "" != table_size_join_str:
                        table_size_join_str = table_size_join_str + ","

                    table_size_join_str = table_size_join_str + table_size_str
                    f.close()
                    break
                current_line = f.readline()
        return table_size_join_str

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_name", help="kudu table name", dest="table_name", type=str, default="")
    parser.add_argument("--remote_replica_ret_path", help="remote replica ret path", dest="remote_replica_ret_path", type=str, default="")
    args = parser.parse_args()

    input_table_name = args.table_name
    input_remote_replica_ret_path = args.remote_replica_ret_path
    if "" == input_table_name or "" == input_remote_replica_ret_path:
        print "0"
        sys.exit()
    
    parser = KuduTableRemoteParser(input_table_name, input_remote_replica_ret_path)
    table_size_split_arr = parser.getTableSizeSplitArr()

    tool = KuduTableSizeTool()
    table_total_size = tool.getTotalSizeKBytes(table_size_split_arr)

    print table_total_size

main()