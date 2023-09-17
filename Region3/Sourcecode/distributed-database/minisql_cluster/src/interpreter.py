import sys

import ply.lex as lex
import ply.yacc as yacc

from minisql_cluster.src.facade import MinisqlFacade

reserved = (
    'SELECT', 'CREATE', 'INSERT', 'DELETE', 'DROP', 'TABLE', 'PRIMARY', 'KEY',
    'UNIQUE', 'INT', 'CHAR', 'FLOAT', 'ON', 'FROM', 'QUIT', 'VALUES', 'INTO',
    'INDEX', 'WHERE', 'AND', 'OR', 'EXECUTE',
)

tokens = reserved + \
         (
             # Identifier, including names for table, column, index..
             'ID',

             # Delimeters
             'COMMA', 'LPAREN', 'RPAREN', 'SEMICOLON', 'DOT',

             # Operation
             'LT', 'GT', 'LE', 'GE', 'EQ', 'NE',

             # Literal
             'ICONST', 'SCONST', 'FCONST',

             # Symbol
             'STAR',

         )

t_ignore = ' \t\x0c'

# Symbols
t_STAR = '\*'

# Delimeters
t_COMMA = ','
t_LPAREN = '\('
t_RPAREN = '\)'
t_SEMICOLON = ';'
t_DOT = '\.'

# Operators
t_LT = r'<'
t_GT = r'>'
t_LE = r'<='
t_GE = r'>='
t_EQ = r'='
t_NE = r'!='


# Floating literal
def t_FCONST(t):
    r'((\d+)(\.\d+)(e(\+|-)?(\d+))? | (\d+)e(\+|-)?(\d+))([lL]|[fF])?'
    t.value = float(t.value)
    return t


# Integer Literal
def t_ICONST(t):
    r'\d+([uU]|[lL]|[uU][lL]|[lL][uU])?'
    t.value = int(t.value)
    return t


# String literal
def t_SCONST(t):
    r'\'([^\\\n]|(\\.))*?\''
    t.value = t.value.strip('\'')
    return t


reserved_map = {}

for r in reserved:
    reserved_map[r.lower()] = r


def t_ID(t):
    r'[A-Za-z_][\w_]*'
    t.type = reserved_map.get(t.value.lower(), "ID")
    return t


def t_error(t):
    add_result('Syntax Error at {}'.format(t.value))
    t.lexer.skip(1)


lexer = lex.lex()


# translation unit

def p_sql_statement(p):
    '''
        sql_statement : create_statement
                        | insert_statement
                        | select_statement
                        | delete_statement
                        | drop_statement
                        | quit_statement
                        | execute_statement
    '''
    pass


def p_create_statement(p):
    '''
        create_statement : create_table
                          | create_index
    '''
    type_code = p[1]['type']
    if type_code == 'create_index':
        MinisqlFacade.create_index(p[1]['table_name'], p[1]['index_name'], p[1]['column_name'])
        add_result('create index successfully!')
        set_result_flag()
    elif type_code == 'create_table':
        try:
            if p[1]['primary']:
                MinisqlFacade.create_table(p[1]['table_name'], p[1]['primary key'], p[1]['element_list'])
            else:
                MinisqlFacade.create_table(p[1]['table_name'], None, p[1]['element_list'])
            add_result('create table successfully!')
            set_result_flag()
        except ValueError as value_error:
            add_result('Error! {}'.format(value_error))


def p_insert_statement(p):
    '''
        insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN SEMICOLON
    '''
    table_name = p[3]
    value_list = p[6]
    try:
        MinisqlFacade.insert_record(table_name, value_list)
        add_result('insert successfully!')
        set_result_flag()
    except KeyError as key_error:
        add_result('Insertion failed.')
        add_result('Error message: No table {}'.format(key_error))
    except Exception as ex:
        add_result('Insertion failed.')
        add_result('Error message:  ' + str(ex))


def p_select_statement(p):
    '''
        select_statement : select_all
                        | conditional_select
    '''
    type_code = p[1]['type']
    try:
        columns = MinisqlFacade.get_columns_name(p[1]['table_name'])
        columns_format = ' | '.join(column for column in columns)
        if type_code == 'select_all':
            records = MinisqlFacade.select_record_all(p[1]['table_name'])
        else:  # conditional select
            records = MinisqlFacade.select_record_conditionally(p[1]['table_name'], p[1]['conditions'])
        add_result('*****' * len(columns))
        add_result(columns_format)
        add_result('*****' * len(columns))
        for record in records:
            record_str = ' | '.join(str(item) for item in record)
            add_result(record_str)
        set_result_flag()
    except KeyError:
        add_result('Error! The table {} is not exist!'.format(p[1]['table_name']))


def p_delete_statement(p):
    '''
        delete_statement : delete_all
                        | conditional_delete
    '''
    type_code = p[1]['type']
    if type_code == 'delete_all':
        try:
            MinisqlFacade.delete_record_all(p[1]['table_name'])
            add_result('delete all successfully!')
            set_result_flag()
        except Exception as ex:
            add_result('Error! ' + str(ex))
    elif type_code == 'conditional_delete':
        MinisqlFacade.delete_record_conditionally(p[1]['table_name'], p[1]['conditions'])
        add_result('delete successfully!')
        set_result_flag()


def p_drop_statement(p):
    '''
        drop_statement : drop_table
                        | drop_index
    '''
    type_code = p[1]['type']
    if type_code == 'drop_table':
        try:
            MinisqlFacade.drop_table(p[1]['table_name'])
            add_result('drop table successfully!')
            set_result_flag()
        except ValueError as value_error:
            add_result('Error! ' + str(value_error))
        except KeyError:
            add_result('Error! There is no table {}.'.format(p[1]['table_name']))
    elif type_code == 'drop_index':
        try:
            MinisqlFacade.drop_index(p[1]['index_name'])
            add_result('drop index successfully!')
            set_result_flag()
        except Exception as ex:
            add_result('Error! ' + str(ex))


def p_quit_statement(p):
    '''
        quit_statement : QUIT SEMICOLON
    '''
    add_result('bye bye!')
    MinisqlFacade.quit()
    sys.exit()


# Rules for create statement
def p_create_table(p):
    '''
        create_table : CREATE TABLE ID LPAREN column_list RPAREN SEMICOLON
                    | CREATE TABLE ID LPAREN column_list COMMA primary_clause RPAREN SEMICOLON
    '''
    dict = {}
    dict['type'] = 'create_table'
    dict['table_name'] = p[3]
    dict['element_list'] = p[5]
    if len(p) == 8:
        dict['primary'] = False
    elif len(p) == 10:
        dict['primary'] = True
        dict['primary key'] = p[7]
    p[0] = dict


def p_create_index(p):
    '''
        create_index : CREATE INDEX ID ON ID LPAREN ID RPAREN SEMICOLON
    '''
    dict = {}
    dict['type'] = 'create_index'
    dict['index_name'] = p[3]
    dict['table_name'] = p[5]
    dict['column_name'] = p[7]
    p[0] = dict


def p_column_list(p):
    '''
        column_list : column
                    | column_list COMMA column
    '''
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[3])


def p_column(p):
    '''
        column :  ID column_type
                | ID column_type UNIQUE
    '''
    if len(p) == 4:
        p[0] = (p[1], p[2], True)
    else:
        p[0] = (p[1], p[2], False)


def p_column_type(p):
    '''
        column_type : INT
                    | FLOAT
                    | CHAR LPAREN ICONST RPAREN
    '''
    type_code = p[1].lower()
    if type_code == 'int':
        p[0] = ('int', 1)
    elif type_code == 'float':
        p[0] = ('float', 1)
    elif type_code == 'char':
        p[0] = ('char', p[3])


def p_primary_clause(p):
    '''
        primary_clause : PRIMARY KEY LPAREN ID RPAREN
    '''
    p[0] = p[4]  # the column name


# Rules for insert statement
def p_value_list(p):
    '''
        value_list : value
                    | value_list COMMA value
    '''
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[3])


def p_value(p):
    '''
        value : ICONST
                | FCONST
                | SCONST
    '''
    p[0] = p[1]


# Rules for select statement
def p_select_all(p):
    '''
        select_all : SELECT STAR FROM ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'select_all'
    dict['table_name'] = p[4]
    p[0] = dict


def p_conditional_select(p):
    '''
        conditional_select : SELECT STAR FROM ID WHERE conditions SEMICOLON
    '''
    dict = {}
    dict['type'] = 'conditional_select'
    dict['table_name'] = p[4]
    dict['conditions'] = p[6]
    p[0] = dict


def p_conditions(p):
    '''
        conditions : condition
                    | conditions AND condition
                    | conditions OR condition
    '''
    p[0] = []  # [condition, AND/OR, condition, AND/OR.....]
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[2])
        p[0].append(p[3])


def p_condition(p):
    '''
        condition :  ID GT value
                    | ID LT value
                    | ID EQ value
                    | ID GE value
                    | ID LE value
                    | ID NE value
    '''
    p[0] = (p[1], p[2], p[3])


# Rules for delete statement

def p_delete_all(p):
    '''
        delete_all : DELETE FROM ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'delete_all'
    dict['table_name'] = p[3]
    p[0] = dict


def p_conditional_delete(p):
    '''
        conditional_delete : DELETE FROM ID WHERE conditions SEMICOLON
    '''
    dict = {}
    dict['type'] = 'conditional_delete'
    dict['table_name'] = p[3]
    dict['conditions'] = p[5]
    p[0] = dict


# Rules for drop statement
def p_drop_table(p):
    '''
        drop_table : DROP TABLE ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'drop_table'
    dict['table_name'] = p[3]
    p[0] = dict


def p_drop_index(p):
    '''
        drop_index : DROP INDEX ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'drop_index'
    dict['index_name'] = p[3]
    p[0] = dict


def p_execute_statement(p):
    '''
        execute_statement : EXECUTE ID SEMICOLON
                            | EXECUTE ID DOT ID SEMICOLON
    '''
    if len(p) == 4:
        filename = p[2]
    else:
        filename = p[2] + '.' + p[4]
    file_path = './scripts/' + filename
    with open(file_path, 'r') as file:
        lines = (line.strip() for line in file.readlines())
        sql = ''
        for line in lines:
            if line.rstrip().endswith(';'):
                sql += ' ' + line
                parser.parse(sql)
                sql = ''
            else:
                sql += ' ' + line


# Others
def p_error(p):
    add_result('Syntax error!')


parser = yacc.yacc(method='LALR')


# End of Lexer and Parser

def cmd_get_sql():
    sql = ''
    s = input('MiniSQL>  ')
    while True:
        if s.rstrip().endswith(';'):
            sql += ' ' + s
            return sql
        else:
            sql += ' ' + s
            s = input()


result = bytearray()
result_flag = False


def set_result_flag():
    global result_flag
    result_flag = True

def get_result_flag():
    global result_flag
    return result_flag


def add_result(s):
    global result
    result += bytearray(s + "\n", encoding="utf-8")

def get_result():
    global result
    return result

def clear_result():
    global result
    result = bytearray()

def zookeeper_result(zk, result_path, server_name):
    global result
    zk.set(result_path, bytes(bytearray(server_name + ":\n", encoding="utf-8") + result))
    result = bytearray()


if __name__ == '__main__':
    while True:
        sql = cmd_get_sql()
        parser.parse(sql)
