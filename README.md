# kudu analyse tool

## env requirement
kudu version >= 1.7
centos7
python 2.7

python requirement:
pip install bs4
pip install lxml

## propable impact
This shell will send kudu analyse command to each tablet server, and mainly require cpu resource. The machine which execute this tools do not need so much resource.

## files
### env.sh
env setting
- kudu server port
- to analyse table name
- kudu data and metadata path

### log.sh
logger
you can log_level to control logging level (DEBUG/INFO/WARNING/ERROR)
log will be saved in logs folder

### kudu_table_summary.sh
get table summary
- table size
- tablet count
- last modify date

### kudu_table_id_parser.py
get kudu table id by kudu name from kudu web
can be tested like this:
python kudu_table_id_parser.py --table_name=...

## how to use
make run name = 'impala::db.table'
or: 
make run
(need configure search_table_name in env.sh)

## not yet implement
...
