#%%TODO library
import re
import sys
import textwrap


#%%TODO sys vars
#python Shell_to_SQL.py "C:\Users\jjuua\PycharmProjects\Prueba\J093168_KPIGRP019.sh"

path_sh_file = sys.argv[1]

#%%TODO Funcs
def read_file(path_sh_file):
    with open(path_sh_file, 'r') as file:
        string = file.read()
    return string

def extract_sql(sql_pattern, string):
    # Define regex pattern to match BTEQ commands
    bteq_pattern = r"((?:\n|^)\s*\.[^;]*;)"
    string = re.sub(bteq_pattern, "", string)
    # Use re.findall() to extract all matches
    sql_statements = re.findall(sql_pattern, string, flags=re.IGNORECASE | re.MULTILINE)
    # Use lstrip() to remove leading whitespace from each line
    sql_statements = [line.lstrip() for line in sql_statements]
    # Join the SQL statements back into a single string
    sql_string = "\n".join(sql_statements)

    return sql_string

def replace_values(dict_vars, sql_string):
    for key, value in dict_vars.items():
        sql_string = re.sub("\${"+key+"}", value, sql_string)
    sql_string = textwrap.dedent(sql_string)
    return sql_string

def save_sql(path_sql_file, sql_string):
    with open(path_sql_file, 'w') as file:
        file.write(sql_string)
    print("SQL file saved in: ",path_sql_file)


#%%TODO Program
# Define the regular expression to match SQL statements
#sql_pattern = r"^(\s*SELECT[\s\S]*?;\n|\s*INSERT[\s\S]*?;\n|\s*UPDATE\s.+?;|\s*DELETE[\s\S]*?;\n|\s*CREATE[\s\S]*?;\n|ALTER\s.+?;|\s*DROP\s.+?;|\s*TRUNCATE\s.+?;)"
sql_pattern = r"^(?:\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)[\s\S]*?;\n|\s*--.*\n|\s*/\*[\s\S]*?\*/)"

path_sql_file = path_sh_file.replace(".log",".sql")

string = read_file(path_sh_file)
sql_string = extract_sql(sql_pattern, string)

save_sql(path_sql_file, sql_string)

