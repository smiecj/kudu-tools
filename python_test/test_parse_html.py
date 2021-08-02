#!/usr/bin/python
# -*- coding: UTF-8 -*

import sys
from kudu_table_id_parser import KuduTableParser

kudu_host = "kudu_host"
kudu_port = "8051"
search_kudu_table = "impala::default.test_table_name"

parser = KuduTableParser(kudu_host, kudu_port, search_kudu_table, "")
print parser.getIdByName()