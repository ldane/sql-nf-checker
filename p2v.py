#!/usr/bin/env python

import vertica_python
import sys

class Table:
    table_name = ""
    key_list = list()
    nonkey_list = list()

    def __init__(self, table_schema):
        #find table name 
        table_name_index = table_schema.find('(')
        # if the schema is wrong and has no ( 
        if table_name_index == -1:
            return
        rest_of_string = table_schema[table_name_index:]
        first = table_schema.find('(')
        last = table_schema.rfind(')')
        final_string = table_schema[first+1:last]

        # assigned table_name
        self.table_name = table_schema[:table_name_index]

        # split by ,
        columns = final_string.split(',')
        self.key_list = list()
        self.nonkey_list = list()
        for col in columns:
            col_stripped = col.strip()
            if '(k)' in col_stripped:
                self.key_list.append(col_stripped.replace('(k)', ''))
            else:
                self.nonkey_list.append(col_stripped)

    def _string_check(self, target):
        import string
        charset = string.ascii_letters+'0123456789_'
        for c in set(target):
            if c not in charset:
                return False
        return True

    # check for anomalies in names 
    def check_name_validity(self):
        #return true if all names valid table/column names
        #returns false if table breaks some standard format
        for i in [self.table_name] + self.key_list + self.nonkey_list:
            if not self._string_check(i):
                return False
        return True

def check_nf(my_table, my_cursor):
    #returns a list of booleans representing normal form checks and a string describing the reason for failure, and a boolean describing table failure
    #[1nf, 2nf, 3nf, bcnf], 'reason'
    #check if all table columns are valid
    is_1nf, is_2nf, is_3nf, is_bcnf = False, False, False, False
    statement = 'SELECT * FROM ' + my_table.table_name
    execute_statement(my_cursor, statement, statement)
    try:
        column_list = [desc.name.lower() for desc in my_cursor.description]
    except Exception as e:
        print(e)
        return [is_1nf, is_2nf, is_3nf, is_bcnf], 'Invalid table columns', True
    for attribute in my_table.key_list + my_table.nonkey_list:
        if attribute.lower() not in column_list:
            return [is_1nf, is_2nf, is_3nf, is_bcnf], 'table column in query do not match table: ' + attribute, True
    
    reason = ''
    is_1nf, reason = check_1nf(my_table, my_cursor)
    if is_1nf:
        is_2nf, reason = check_2nf(my_table, my_cursor)
        if is_2nf:
            is_3nf, reason = check_3nf(my_table, my_cursor)
            if is_3nf:
                is_bcnf, reason = check_bcnf(my_table, my_cursor)
    #testing
    return [is_1nf, is_2nf, is_3nf, is_bcnf], reason, False

def check_1nf(my_table, my_cursor):
    # returns True, '' if passes 1NF check
    # returns False, 'reason' if fails 1NF check, string will contain reason for failure ie 'duplicate keys' 'null in key'
    # check if any null values in the supposed primary key columns (composite)
	
    string_reason = ''
    if len(my_table.key_list) == 0:
        return False, 'NO PK'
    for key in my_table.key_list:
        statement = 'SELECT COUNT(*) FROM ' + my_table.table_name + ' WHERE ' + key + ' IS NULL'
        formatted_statement = 'SELECT COUNT(*) FROM ' + my_table.table_name + '\nWHERE ' + key + ' IS NULL'
        execute_statement(my_cursor, statement, formatted_statement)
        try:
            result_data = my_cursor.fetchall()
        except Exception as e:
            # need to catch specific exceptions for useful error output
            print(e)
            return False, 'Invalid table columns or SQL query', True
        #testing return from query
        #for row in result_data:
            #print(row)
        if result_data[0][0] > 0:
            if string_reason != '':
                string_reason += ', '
            string_reason += 'NULL in ' + key

    # check if key has duplicates
    keys_clause = ''
    for key in my_table.key_list:
        if keys_clause != '':
            keys_clause += ', '
        keys_clause += key
    statement = 'SELECT COUNT(*) FROM ' + my_table.table_name + ' GROUP BY ' + keys_clause
    formatted_statement = 'SELECT COUNT(*) FROM ' + my_table.table_name + '\nGROUP BY ' + keys_clause
    execute_statement(my_cursor, statement, formatted_statement)
    result_data = my_cursor.fetchall()
    # testing return from query
    #for row in result_data:
        #print(row)
    for row in result_data:
        if row[0] > 1:
            string_reason += 'DUPLICATE KEY in ' + keys_clause
    if string_reason != '':
        return False, string_reason
    else:
        return True, ''

def check_2nf(my_table, my_cursor):
    #returns boolean, string
    from itertools import combinations
    result = True
    reason = []
    n = len(my_table.key_list)
    if n == 1:
        return True, ''
    for nonkey in my_table.nonkey_list:
        for i in range(1,n):
            for test_case in combinations(my_table.key_list, i):
                test_str=''.join(['%s,' %(j) for j in test_case])[:-1]
                query = 'SELECT COUNT(*) FROM ' + \
                        '(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, nonkey) + \
                        'as c FROM %s ' %(my_table.table_name) + \
                        'WHERE %s is NOT NULL ' %(nonkey) + \
                        'GROUP BY %s) as t ' %(test_str) + \
                        'WHERE c!=1;'
                formatted_query = 'SELECT COUNT(*) FROM ' + \
                                  '\n\t(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, nonkey) + \
                                  'as c FROM %s ' %(my_table.table_name) + \
                                  '\n\tWHERE %s is NOT NULL ' %(nonkey) + \
                                  '\n\tGROUP BY %s) as t ' %(test_str) + \
                                  '\nWHERE c!=1;'
                execute_statement(my_cursor, query, formatted_query)
                try:
                    result_data = my_cursor.fetchall()
                except Exception as e:
                    # need to catch specific exceptions for useful error output
                    print(e)
                    return False, 'Invalid table columns or SQL query', True
                if result_data[0][0] == 0:
                    result=False
                    reason.append('%s->%s' %(test_str, nonkey))
    if result:
        reason=''
    else:
        reason = ', '.join(reason)
    return result, reason

def check_3nf(my_table, my_cursor):
    #returns boolean, string
    from itertools import combinations
    result = True
    reason = []
    for nonkey in my_table.nonkey_list:
        targetkeys = list(my_table.nonkey_list)
        targetkeys.remove(nonkey)
        n = len(targetkeys)
        for i in range(1,n+1):
            for test_case in combinations(targetkeys, i):
                test_str=''.join(['%s,' %(j) for j in test_case])[:-1]
                keys = [nonkey]+ list(test_case)
                query = 'SELECT COUNT(*) FROM ' + \
                        '(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, nonkey) + \
                        'as c FROM %s ' %(my_table.table_name) + \
                        'WHERE ' + ' AND '.join([k+' IS NOT NULL' for k in keys]) + \
                        ' GROUP BY %s) as t ' %(test_str) + \
                        'WHERE c!=1;'
                formatted_query = 'SELECT COUNT(*) FROM ' + \
                                  '\n\t(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, nonkey) + \
                                  'as c FROM %s ' %(my_table.table_name) + \
                                  '\n\tWHERE ' + ' AND '.join([k+' IS NOT NULL' for k in keys]) + \
                                  '\n\tGROUP BY %s) as t ' %(test_str) + \
                                  '\nWHERE c!=1;'
                execute_statement(my_cursor, query, formatted_query)
                try:
                    result_data = my_cursor.fetchall()
                except Exception as e:
                    # need to catch specific exceptions for useful error output
                    print(e)
                    return False, 'Invalid table columns or SQL query'
                if result_data[0][0] == 0:
                    result=False
                    reason.append('%s->%s' %(test_str, nonkey))
    if result:
        reason=''
    else:
        reason = ','.join(reason)
    return result, reason
 
def check_bcnf(my_table, my_cursor):
    #returns boolean, string
    from itertools import combinations
    result = True
    reason = []
    m = len(my_table.key_list)
    if m == 1:
        return True, ''
    n = len(my_table.nonkey_list)
    for key in my_table.key_list:
        for i in range(1,3):
            for test_case in combinations(my_table.nonkey_list, i):
                test_str=''.join(['%s,' %(j) for j in test_case])[:-1]
                query = 'SELECT COUNT(*) FROM ' + \
                        '(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, key) + \
                        'as c FROM %s ' %(my_table.table_name) + \
                        'WHERE %s is NOT NULL ' %(key) + \
                        'GROUP BY %s) as t ' %(test_str) + \
                        'WHERE c!=1;'
                formatted_query = 'SELECT COUNT(*) FROM ' + \
                                  '\n\t(SELECT %s, COUNT(DISTINCT %s) ' % (test_str, key) + \
                                  'as c FROM %s ' %(my_table.table_name) + \
                                  '\n\tWHERE %s is NOT NULL ' %(key) + \
                                  '\n\tGROUP BY %s) as t ' %(test_str) + \
                                  '\nWHERE c!=1;'
                execute_statement(my_cursor, query, formatted_query)
                result_data = my_cursor.fetchall()
                if result_data[0][0] == 0:
                    result=False
                    reason.append('%s->%s' %(test_str, key))
    if result:
        reason=''
    else:
        reason = ', '.join(reason)
    return result, reason

def execute_statement(my_cursor, my_statement, my_formatted_statement):
    # executes sql statement and writes to file
    try:
        my_cursor.execute(my_statement)
    except Exception as e:
        print(e)

    # before writing to file, separate the statement's WHERE JOIN GROUP clause.
    #statement1 = my_statement.replace('WHERE', '\n\tWHERE')
    #statement2 = statement1.replace('GROUP', '\n\tGROUP')
    #statement3 = statement2.replace('INNER JOIN', '\n\tINNER JOIN')
    #statement4 = statement3.replace('(SELECT', '\n\t(SELECT')

    with open ('NF.sql', 'a') as f_sql:
        f_sql.write(my_formatted_statement + '\n\n')

def print_row(my_table_name, nf_boolean_list, my_reason, table_failure = False):
    #first print table name, then print which NF fails if any, if there is a failure then the myReason string is not empty
    failed = ''
    
    #check length of reason is < 200
    truncated_reason = (my_reason[:200] + '...') if len(my_reason) > 200 else my_reason
    
    if table_failure == True:
        failed = '---'
    elif nf_boolean_list[0] == False:
        failed = '1NF'
    elif nf_boolean_list[1] == False:
        failed = '2NF'
    elif nf_boolean_list[2] == False:
        failed = '3NF'
    elif nf_boolean_list[3] == False:
        failed = 'BCNF'
    if (len(my_table_name) < 8):
        finalized_reason = my_table_name + '\t\t' + failed + '\t\t' + truncated_reason
    else:
        finalized_reason = my_table_name + '\t' + failed + '\t\t' + truncated_reason
    
    print(finalized_reason)
    # write to file NF.txt
    with open ('NF.txt', 'a') as f_txt:
        f_txt.write(finalized_reason + '\n')

def main():
    # file login.ini contains host, username, password, and db name
    with open('login.ini', 'r') as f:
        host = f.readline().strip()
        username = f.readline().strip()
        password = f.readline().strip()
        database = f.readline().strip()

    conn_info = {'host': host,
                 'port': 5433,
                 'user': username,
                 'password': password,
                 'database': database,
                 'read_timeout': 600,
                 'connection_timeout': 5}


    # clear the log files
    open ('NF.sql', 'w').close()
    open ('NF.txt', 'w').close()

    # grab input from command line argument
    # only 1 argument allowed
    if len(sys.argv) != 2:
        print('Invalid input - follow format "python p2v.py database=something.txt"')
        return
    descriptor, db_file_name = sys.argv[1].split('=')
    lines = [line.rstrip('\r\n') for line in open(db_file_name)]

    # connect to database
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()

    print('#Table\t\tFailed\t\tReason')
    with open ('NF.txt', 'a') as f_txt:
        f_txt.write('#Table\t\tFailed\t\tReason\n')
    

    # from the schema, evaluate each line into the table class which forms a key and non key list
    for line in lines:
        # If there is empty line continue
        if not line:
            continue
        temp_table = Table(line)
        if (temp_table.check_name_validity()):
            #table names are valid, now check normal form
            normal_forms, reason, table_failure = check_nf(temp_table, cur)
            print_row(temp_table.table_name, normal_forms, reason, table_failure)
        else: 
            print('Invalid table was found. \t' + line)
    #dataset = 'Employees'
    #stm = 'SELECT * FROM Employees'
    #cur.execute(stm)
    #execute_statement(cur, stm)

    #for row in cur.iterate():
        #print(row)

    #return_data = cur.fetchall()
    #print return_data[0]
    #print return_data[0][0]

if __name__ == "__main__":
    main()

# vim: ts=4 sw=4 et nowrap 
