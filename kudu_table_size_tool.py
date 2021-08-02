#!/usr/bin/python
# -*- coding: UTF-8 -*-

""" KuduTableSizeParser: kudu table size statistician
"""

import argparse

class KuduTableSizeTool:
    def __init__(self):
        pass

    def _transform_size(self, sizeStr):
        if len(sizeStr) == 0:
            return 0
        sizeNum = float(sizeStr[:len(sizeStr)-1])
        sizeNum = {
            'G': sizeNum * 1024 * 1024,
            'M': sizeNum * 1024,
            'K': sizeNum,
            'B': sizeNum / 1024,
        }.get(sizeStr[len(sizeStr)-1], 0)
        return sizeNum
    
    ## add all table size(format like this: 215B,157B,140B,171B,147B,165B) and return table size in kb unit
    def getTotalSizeKBytes(self, sizeArrStr):
        sizeArr = sizeArrStr.split(',')
        totalSize = 0
        for sizeStr in sizeArr:
            sizeNum = self._transform_size(sizeStr)
            totalSize += sizeNum
        return totalSize

def main():
    ## table_size_str: table's all tablet size come from kudu tablet server
    ## file_name: table's all tablet size, also come from kudu tablet server but read from file to avoid shell's parameter too long
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_size_str", help="kudu table size string", dest="table_size_str", type=str, default="")
    parser.add_argument("--file_name", help="kudu table size string store file", dest="file_name", type=str, default="")
    args = parser.parse_args()

    input_table_size_str = args.table_size_str
    input_file_name = args.file_name
    if "" == input_table_size_str and "" == input_file_name:
        print "0"
        sys.exit()
    tool = KuduTableSizeTool()

    if "" != input_file_name:
        f = open(input_file_name)
        input_table_size_str = f.readline().strip()
        f.close()

    print tool.getTotalSizeKBytes(input_table_size_str)

if __name__ == "__main__":
    main()