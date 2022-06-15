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

## usage
### analyse table size
make summary name = 'impala::db.table'
or: 
make summary
(need configure search_table_name in env.sh)

### export and import table structure
make db_name="export_db_name" export_table

make db_name="import_db_name" import_table

### backup kudu table data
make source_db="source_db_name" target_db="target_db_name" backup_table

### check kudu table
make db_name="db_name" source_db_name="stg" check_table

## not yet implement
- create sync task on datalink
