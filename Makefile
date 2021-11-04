run:
	sh main.sh $(name)

export_table:
	sh export_table.sh $(db_name)

import_table:
	sh import_table.sh $(db_name)

test_load_json:
	python python_test/test_load_json.py

test_parse_html:
	python -m python_test.test_parse_html

test_parse_size:
	python -m kudu_table_size_tool --table_size_str="119B,1.5K,0B,4.1K,0B,16.1K"

clean:
	sh clean.sh