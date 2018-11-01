from typing import Dict, List, Any, Union, Tuple
# import psycopg2
# bkp_folder = "/Users/rogov/work/gi/bkp_json"

# file_name="user_add_datastream_2018-09-19_node1-datastream-prod_1537405202.dump.gz"
# file_name = open('user_info.dump', 'r', encoding='UTF-8')
# er_test = open('errors_test_rows.py', 'w', encoding='UTF-8')
# for line in gzip.open("{}/{}".format(bkp_folder, file_name), 'r').readlines():
# file_name = open('user_info2.dump', 'r', encoding='UTF-8')
# sys.stdin.readlines()


import demjson
import sys





for line in sys.stdin.readlines():
    new_row = {}
    row = line.split("\t")
    new_row["event_id"] = row[0]
    new_row["event_type"] = row[1]
    new_row["project"] = row[2]
    with open('errors_%s.dump' % new_row["project"], 'a', encoding='UTF-8') as er_test:
        try:
            data = demjson.decode(row[3])
        except Exception:
            er_test.write(demjson.encode(line))
    new_row['event_ts'] = row[4]
    new_row["event_info"] = row[5]
    new_row["ins_id"] = row[6]
    new_row["ins_ts"] = row[7].replace("\n", "")

    for row_data in data:
        if isinstance(data[row_data], dict):
            for row_in_dict in data[row_data]:
                new_row["{}_{}".format(row_data, row_in_dict)] = data[row_data][row_in_dict]
    else:
        new_row[row_data] = data[row_data]

    a = list(new_row.keys())
    a.sort()
    new_row["row_schema"] = a
    # print(new_row)
    prj_name = new_row["project"]
    with open('./data/%s.dump' % prj_name, '+a', encoding='UTF-8') as f_pr:
        f_pr.write(demjson.encode(new_row))
        f_pr.write("\n")


