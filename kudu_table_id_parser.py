#!/usr/bin/python
# -*- coding: UTF-8 -*-

""" KuduTableParser: kudu table id getter, by parse kudu master web's html body
env requirement:
python2
pip

pip install bs4
pip install lxml
"""

import argparse
import sys, urllib
from bs4 import BeautifulSoup

## KuduTableParser
class KuduTableParser:
    def __init__(self, kudu_host, kudu_port, table_name, table_id):
        self.host = kudu_host
        self.port = kudu_port
        self.name = table_name
        self.id = table_id

        self.web_tables_url = "http://{}:{}/tables".format(kudu_host, kudu_port)

    
    def getIdByName(self):
        get_tables_ret = urllib.urlopen(self.web_tables_url)
        soup = BeautifulSoup(get_tables_ret.read(), "lxml")
        find_table_ret = soup.find_all("table", "table table-striped")

        if find_table_ret is None or 0 == len(find_table_ret):
            return "error: get kudu table info failed: kudu node {} maybe not master node".format(self.host)

        table = find_table_ret[0]

        find_tr_ret = table.find_all("tr")
        
        for table in find_tr_ret:
            tag_table_name = table.find("th")
            tag_table_link = table.find("a")

            if tag_table_link is None:
                continue
            if tag_table_name.string == self.name:
                return tag_table_link.string
        return "error: get kudu table info failed: maybe network error or there is no table named " + self.name
    def getNameById(self):
        return "error: method not defined"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_name", help="kudu table name", dest="table_name", type=str, default="")
    parser.add_argument("--table_id", help="kudu table id", dest="table_id", type=str, default="")
    parser.add_argument("--kudu_host", help="kudu host", dest="kudu_host", type=str, default="")
    parser.add_argument("--kudu_port", help="kudu port", dest="kudu_port", type=str, default="")
    args = parser.parse_args()

    input_table_name = args.table_name
    input_table_id = args.table_id
    input_kudu_host = args.kudu_host
    input_kudu_port = args.kudu_port
    if "" == input_table_name:
        print ""
        sys.exit()
    parser = KuduTableParser(input_kudu_host, input_kudu_port, input_table_name, input_table_id)

    print parser.getIdByName()

if __name__ == "__main__":
    main()