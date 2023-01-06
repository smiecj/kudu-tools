actual_table_name ?=
export

summary:
	sh table_size_summary.sh $(name)

export_table:
	sh export_table.sh $(db_name)

import_table:
	sh import_table.sh $(db_name)

transform_table:
	sh transform_table.sh $(source_db) $(target_db)

backup_table:
	sh backup_table.sh $(source_db) $(target_db)

drop_table:
	sh drop_table.sh $(db) $(table)

check_table:
	sh check_table.sh $(db_name) $(source_db)

transform_exec:
	sh transform_and_execute.sh $(source_db) $(target_db) ${target_table}

test_load_json:
	python python_test/test_load_json.py

test_parse_html:
	python -m python_test.test_parse_html

test_parse_size:
	python -m kudu_table_size_tool --table_size_str="119B,1.5K,0B,4.1K,0B,16.1K"

clean:
	sh clean.sh