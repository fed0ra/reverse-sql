#!/usr/bin/env python3

# pip install PyMySQL mysql-replication pytz

import argparse
import pymysql
import sys
import time
import datetime
import threading
import pytz
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, wait
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

timezone = pytz.timezone('Asia/Shanghai')

# 定义队列对象，存放SQL和回滚SQL
result_queue = Queue()
result_queue_replace = Queue()
# 定义列表，存放result_queue和result_queue_replace两个队列中的数据
combined_array = []
combined_array_replace = []

# 创建一个锁对象
file_lock = threading.Lock()

# 检查binlog_format值是否为ROW，binlog_row_image值是否为FULL，不满足条件退出
def check_binlog_settings(mysql_host="192.168.133.151", mysql_port=3306, mysql_user="root",
                          mysql_passwd="Admin@2023", mysql_database="test", mysql_charset="utf8mb4"):
    # 连接MySQL数据库
    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "database": mysql_database,
        "charset": mysql_charset
    }

    conn = pymysql.connect(**source_mysql_settings)
    cursor = conn.cursor()

    try:
        # 查询binlog_format的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        row = cursor.fetchone()
        binlog_format = row[1]

        # 查询binlog_row_image的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        row = cursor.fetchone()
        binlog_row_image = row[1]

        # 检查参数值是否满足条件
        if binlog_format != 'ROW' and binlog_row_image != 'FULL':
            exit("\nMySQL的变量参数binlog_format的值应为ROW，参数binlog_row_image的值应为FULL\n")
            
    finally:
        # 关闭数据库连接
        cursor.close()
        conn.close()

# 处理binlog日志
def process_binlogevent(binlogevent, start_time, end_time):
    # 获取数据库名
    database_name = binlogevent.schema
    # 调试输出
    event = {"schema": binlogevent.schema, "table": binlogevent.table, "log_pos": binlogevent.packet.log_pos, "event_size": binlogevent.event_size}
    # print("*" * 50)
    # print(event)
    # print("database_name: ", database_name, "\nbinlogevent: ", binlogevent, "\nstart time: ", start_time, "\nbinlogevent timestamp: ", binlogevent.timestamp, "\nend time: ", end_time)
    # print("*" * 50)
    # 筛选起始时间内发生的事务
    if start_time <= binlogevent.timestamp <= end_time:
        # binlogevent.dump()    
        for row in binlogevent.rows:
            # 记录事件时间
            event_time = binlogevent.timestamp
            # 判断binlogevent是否为WriteRowsEvent类的实例
            if isinstance(binlogevent, WriteRowsEvent):
                # 判断如果only_operation存在且不是insert操作则跳过迭代
                if only_operation and only_operation != 'insert':
                    continue
                # only_operation不存在，或者存在且是insert操作则执行如下操作
                else:
                    # 拼接原始SQL（表名、列名、值）
                    sql = "INSERT INTO {}({}) VALUES ({});".format(
                        # 如果database_name为空则表名为binlogevent.table，否则表名为`{database_name}`.`{binlogevent.table}`
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        # 取字典中所有key，以``,``格式生成字符串作为列名
                        ','.join(["`{}`".format(k) for k in row["values"].keys()]),
                        # 取字典中所有value，以``,``格式生成字符串作为值，如果值是字符串或日期时间类型，则使用单引号''包围值，如果值为None，则使用'NULL'，否则将值转换为字符串
                        ','.join(["'{}'".format(v) if isinstance(v, (
                        str, datetime.datetime, datetime.date)) else 'NULL' if v is None else str(v)
                        for v in row["values"].values()])
                    )
                    # 拼接回滚SQL
                    rollback_sql = "DELETE FROM {} WHERE {};".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        ' AND '.join(["`{}`={}".format(k, "'{}'".format(v)
                        if isinstance(v, (str, datetime.datetime, datetime.date)) else 'NULL' if v is None else str(v))
                        for k, v in row["values"].items()]))

                    # 调试输出
                    # print("\nupdate: ", row)
                    # event["action"] = "insert"
                    # event["values"] = dict(row["values"].items())
                    # print("\nwrite: ", event, "key: ", row["values"].keys())
                    # print("\n原始SQL: ", sql, "\n回滚SQL: ", rollback_sql)

                    # 将数据放入队列中，供其他线程或者进程使用
                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})
                    # 调用队列
                    # print(result_queue.get())
                    
            # 判断binlogevent是否为UpdateRowsEvent类的实例
            elif isinstance(binlogevent, UpdateRowsEvent):
                # 判断如果only_operation存在且不是update操作则跳过迭代
                if only_operation and only_operation != 'update':
                    continue
                # only_operation不存在，或者存在且是update操作则执行如下操作
                else:
                    set_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, (str, datetime.datetime, datetime.date)):
                            set_values.append(f"`{k}`='{v}'")
                        else:
                            set_values.append(f"`{k}`={v}" if v is not None else f"`{k}`= NULL")
                    set_clause = ','.join(set_values)
                    
                    where_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, (str, datetime.datetime, datetime.date)):
                            where_values.append(f"`{k}`='{v}'")
                        else:
                            where_values.append(f"`{k}`={v}" if v is not None else f"`{k}` IS NULL")
                    where_clause = ' AND '.join(where_values)

                    # UPDATE `test`.`t1` SET `c1`=18,`c2`='王五' WHERE `c1`=18 AND `c2`='None';
                    sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {set_clause} WHERE {where_clause};"

                    rollback_set_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, (str, datetime.datetime, datetime.date)):
                            rollback_set_values.append(f"`{k}`='{v}'")
                        else:
                            rollback_set_values.append(f"`{k}`={v}" if v is not None else f"`{k}`=NULL")
                    rollback_set_clause = ','.join(rollback_set_values)

                    rollback_where_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, (str, datetime.datetime, datetime.date)):
                            rollback_where_values.append(f"`{k}`='{v}'")
                        else:
                            rollback_where_values.append(f"`{k}`={v}" if v is not None else f"`{k}` IS NULL")
                    rollback_where_clause = ' AND '.join(rollback_where_values)

                    rollback_sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {rollback_set_clause} WHERE {rollback_where_clause};"

                    # update转换为replace
                    try:
                        rollback_replace_set_values = []
                        for v in row["before_values"].values():
                           if v is None:
                               rollback_replace_set_values.append("NULL")
                           elif isinstance(v, (str, datetime.datetime, datetime.date)):
                               rollback_replace_set_values.append(f"'{v}'")
                           else:
                               rollback_replace_set_values.append(str(v))
                        rollback_replace_set_clause = ','.join(rollback_replace_set_values)
                        fields_clause = ','.join([f"`{k}`" for k in row["after_values"].keys()])
                        rollback_replace_sql = f"REPLACE INTO `{database_name}`.`{binlogevent.table}` ({fields_clause}) VALUES ({rollback_replace_set_clause});"
                    except Exception as e:
                        print("出现异常错误：", e)
                    
                    # 将数据放入队列中，供其他线程或者进程使用
                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})
                    result_queue_replace.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_replace_sql})
                    # # 调用队列
                    # print(result_queue.get())
                    # print(result_queue_replace.get())

                    # 调试输出
                    # event["action"] = "update"
                    # event["before_values"] = dict(row["before_values"].items())
                    # event["after_values"] = dict(row["after_values"].items())
                    # print("\nupdate: ", event)

            # 判断binlogevent是否为DeleteRowsEvent类的实例
            elif isinstance(binlogevent, DeleteRowsEvent):
                # 判断如果only_operation存在且不是delete操作则跳过迭代
                if only_operation and only_operation != 'delete':
                    continue
                # only_operation不存在，或者存在且是delete操作则执行如下操作
                else:
                    sql = "DELETE FROM {} WHERE {};".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        # 取字典中所有key和value，以``='' AND ``=''格式生成字符串作为列名
                        ' AND '.join(["`{}`={}".format(k, "'{}'".format(v) if isinstance(v, (str, datetime.datetime, datetime.date))
                        else 'NULL' if v is None else str(v))
                        for k, v in row["values"].items()])
                    )

                    rollback_sql = "INSERT INTO {}({}) VALUES ({});".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        '`' + '`,`'.join(list(row["values"].keys())) + '`',
                        ','.join(["'%s'" % str(v) if isinstance(v, (str, datetime.datetime, datetime.date)) else 'NULL' if v is None else str(v)
                        for v in list(row["values"].values())])
                    )

                    # 将数据放入队列中，供其他线程或者进程使用
                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})
                    # 调用队列
                    # print(result_queue.get())

                    # 调试输出
                    # event["action"] = "delete"
                    # event["values"] = dict(row["values"].items())
                    # print("\ndelete: ", event)


def main(mysql_host=None, mysql_port=None, mysql_user=None, mysql_passwd=None,mysql_database=None, mysql_charset=None, 
         only_tables=None, only_operation=None, binlog_file=None, binlog_pos=None, st=None, et=None, max_workers=None, print_output=False, replace_output=False):

    # 判断操作类型    
    valid_operations = ['insert', 'delete', 'update']
    if only_operation:
        only_operation = only_operation.lower()
        if only_operation not in valid_operations:
            print('\n请提供有效的操作类型进行过滤！支持insert、delete、update')
            sys.exit(1)

    # 连接MySQL数据库
    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "database": mysql_database,
        "charset": mysql_charset
    }

    # 创建二进制日志流读取器
    stream = BinLogStreamReader(
        connection_settings=source_mysql_settings,  # 数据库的连接配置信息
        server_id=1234567890,
        blocking=False,             # True，会持续监听binlog时间，False则会一次性解析所有可获取的binlog
        resume_stream=True,         # 从位置或binlog的最新时间或旧的可用时间开始
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],     # 只解析指定的时间，参数为数组类型
        log_file=binlog_file,       # 设置复制开始日志文件
        log_pos=int(binlog_pos),    # 设置复制开始日志pos（resume_stream应为True）
        only_tables=only_tables     # 监听哪些表的操作
    )

    # 时间转换为时间戳
    start_time = int(time.mktime(time.strptime(st, '%Y-%m-%d %H:%M:%S')))
    end_time = int(time.mktime(time.strptime(et, '%Y-%m-%d %H:%M:%S')))
    # 计算时间间隔，将时间范围划分为默认4等份
    interval = (end_time - start_time) // max_workers
    # print("start: ", start_time, "end: ", end_time, "cha: ", (end_time - start_time), "interval: ", interval)

    # 记录上一个任务处理过的binlog_file和binlog_pos
    next_binlog_file = binlog_file
    next_binlog_pos = binlog_pos
    next_binlog_file_lock = threading.Lock()
    next_binlog_pos_lock = threading.Lock()

    # 新建ThreadPoolExecutor线程池对象并指定最大的线程数量
    executor = ThreadPoolExecutor(max_workers=max_workers)

    # 计算每个子时间段的起始时间和结束时间
    for i in range(max_workers):
        task_start_time = start_time + i * interval
        task_end_time = task_start_time + interval
        if i == (max_workers-1):
            # task_end_time = end_time - (max_workers-1) * interval
            task_end_time = end_time
        # print("i: ", i, "new start: ", task_start_time, "new end: ", task_end_time, "i * interval: ", (i * interval))

        # 定义任务列表
        tasks = []
        # 迭代读取时间
        for binlogevent in stream:
            # binlogevent.dump()
            if binlogevent.timestamp < task_start_time:  # 如果事件的时间小于任务的起始时间，则跳过继续迭代下一个事件
                continue
            elif binlogevent.timestamp > task_end_time:  # 如果事件的时间大于任务的结束时间，则结束该任务的迭代
                break

            # process_binlogevent(binlogevent, task_start_time, task_end_time)
            # 多线程处理binlog日志
            task = executor.submit(process_binlogevent, binlogevent, task_start_time, task_end_time)

            # 由于BinLogStreamReader并不支持指定时间戳来进行递增解析，固在每个任务开始之前，使用上一个任务处理过的binlog_file和binlog_pos
            with next_binlog_file_lock:
                if stream.log_file > next_binlog_file:
                    next_binlog_file = stream.log_file

            with next_binlog_pos_lock:
                if stream.log_file == next_binlog_file and stream.log_pos > next_binlog_pos:
                    next_binlog_pos = stream.log_pos

            # 将task对象添加到tasks列表中
            tasks.append(task)

            # 调用队列
            # try:
            #     print(result_queue.get(block=True, timeout=5))
            # except result_queue_replace.empty:
            #     # print(result_queue_replace.get())

        # 等待所有任务完成
        wait(tasks)
        # 关闭读取器
        stream.close()

        # 后续的线程就可以获取到上一个线程处理过的binlog文件名和position，然后从新binlog和position位置读取事件进行后续的并发处理
        stream = BinLogStreamReader(
            connection_settings=source_mysql_settings,
            server_id=1234567890,
            blocking=False,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            log_file=next_binlog_file,
            log_pos=int(next_binlog_pos),
            only_tables=only_tables
        )

    # 处理队列中的结果数据，通过两个while循环分别从result_queue和result_queue_replace中取出数据，并将其添加到combined_array和combined_array_replace列表中    
    while not result_queue.empty():
        combined_array.append(result_queue.get())
    while not result_queue_replace.empty():
        combined_array_replace.append(result_queue_replace.get())

    # 使用sorted函数对这两个列表进行排序，排序的依据是字典中的"event_time"键对应的值，即按照事件时间排序
    sorted_array = sorted(combined_array, key=lambda x: x["event_time"])
    sorted_array_replace = sorted(combined_array_replace, key=lambda x: x["event_time"]) 

    # 获取并格式化当前时间
    c_time = datetime.datetime.now()
    formatted_time = c_time.strftime("%Y-%m-%d_%H:%M:%S")

    # 处理列表中的数据
    for item in sorted_array:
        # 获取转换事件时间
        event_time = item["event_time"]
        dt = datetime.datetime.fromtimestamp(event_time, tz=timezone)
        current_time = dt.strftime('%Y-%m-%d %H:%M:%S')

        sql = item["sql"]
        rollback_sql = item["rollback_sql"]

        if print_output:
            print(f"-- SQL执行时间:{current_time} \n-- 原生sql:\n \t-- {sql} \n-- 回滚sql:\n \t{rollback_sql}\n-- ----------------------------------------------------------\n")

        # 写入文件
        filename = f"{binlogevent.schema}_{binlogevent.table}_recover_{formatted_time}.sql"
        # filename = f"{binlogevent.schema}_{binlogevent.table}.sql"
        with file_lock:  # 获取文件锁
            with open(filename, "a", encoding="utf-8") as file:   
                file.write(f"-- SQL执行时间:{current_time}\n")
                file.write(f"-- 原生sql:\n \t-- {sql}\n")
                file.write(f"-- 回滚sql:\n \t{rollback_sql}\n")
                file.write("-- ----------------------------------------------------------\n")

    if replace_output:
        # update 转换为 replace
        for item in sorted_array_replace:
            event_time = item["event_time"]
            dt = datetime.datetime.fromtimestamp(event_time, tz=timezone)
            current_time = dt.strftime('%Y-%m-%d %H:%M:%S')
    
            sql = item["sql"]
            rollback_sql = item["rollback_sql"]
    
            if print_output:
                print(
                    f"-- SQL执行时间:{current_time} \n-- 原生sql:\n \t-- {sql} \n-- 回滚sql:\n \t{rollback_sql}\n-- ----------------------------------------------------------\n")
    
            # 写入文件
            filename = f"{binlogevent.schema}_{binlogevent.table}_recover_{formatted_time}_replace.sql"
            # filename = f"{binlogevent.schema}_{binlogevent.table}_replace.sql"
            with file_lock:  # 获取文件锁
                with open(filename, "a", encoding="utf-8") as file:
                    file.write(f"-- SQL执行时间:{current_time}\n")
                    file.write(f"-- 原生sql:\n \t-- {sql}\n")
                    file.write(f"-- 回滚sql:\n \t{rollback_sql}\n")
                    file.write("-- ----------------------------------------------------------\n")

    # 关闭读取器
    # stream.close()
    executor.shutdown()

'''
binlogevent.dump()格式

=== WriteRowsEvent ===
Date: 2024-04-16T07:29:34
Log position: 487
Event size: 26
Read bytes: 10
Table: test.t1
Affected columns: 2
Changed rows: 1
Column Name Information Flag: True
Values:
--
* c1 : 15
* c2 : liuzaishi
'''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binlog数据恢复，生成反向SQL语句。", epilog=r"""
Example usage:
    shell> python reverse-sql.py -H 192.168.133.151 -P 3306 -u root -p Admin@2023 -d test -ot t1 -op delete \
            --binlog-file mysql-bin.000124 --start-time "2023-07-06 10:00:00" --end-time "2023-07-06 22:00:00" """,
            formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-H", "--mysql-host", dest="mysql_host", type=str, help="MySQL主机名", required=True)
    parser.add_argument("-P", "--mysql-port", dest="mysql_port", type=int, help="MySQL端口号", required=True)
    parser.add_argument("-u", "--mysql-user", dest="mysql_user", type=str, help="MySQL用户名", required=True)
    parser.add_argument("-p", "--mysql-passwd", dest="mysql_passwd", type=str, help="MySQL密码", required=True)
    parser.add_argument("-d", "--mysql-database", dest="mysql_database", type=str, help="MySQL数据库名", required=True)
    parser.add_argument("-c", "--mysql-charset", dest="mysql_charset", type=str, default="utf8mb4", help="MySQL字符集，默认utf8mb4")
    parser.add_argument("-ot", "--only-tables", dest="only_tables", nargs="+", type=str, help="设置要恢复的表，多张表用,逗号分隔")
    parser.add_argument("-op", "--only-operation", dest="only_operation", type=str, help="设置误操作时的命令（insert/update/delete）")
    parser.add_argument("--binlog-file", dest="binlog_file", type=str, help="Binlog文件", required=True)
    parser.add_argument("--binlog-pos", dest="binlog_pos", type=int, default=4, help="Binlog位置，默认4")
    parser.add_argument("--start-time", dest="st", type=str, help="起始时间", required=True)
    parser.add_argument("--end-time", dest="et", type=str, help="结束时间", required=True)
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=4, help="线程数，默认4（并发越高，锁的开销就越大，适当调整并发数）")
    parser.add_argument("--print", dest="print_output", action="store_true", help="将解析后的SQL输出到终端")
    parser.add_argument("--replace", dest="replace_output", action="store_true", help="将update转换为replace操作")
    args = parser.parse_args()

    # if args.only_tables:
    #     only_tables = args.only_tables[0].split(',') if args.only_tables else None
    # else:
    #     only_tables = None

    if args.only_operation:
        only_operation = args.only_operation.lower()
    else:
        only_operation = None

    # 环境检查
    check_binlog_settings(
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_passwd=args.mysql_passwd,
        mysql_database=args.mysql_database,
        mysql_charset=args.mysql_charset
    )

    main(
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_passwd=args.mysql_passwd,
        mysql_database=args.mysql_database,
        mysql_charset=args.mysql_charset,
        only_tables=args.only_tables,
        only_operation=args.only_operation,
        binlog_file=args.binlog_file,
        binlog_pos=args.binlog_pos,
        st=args.st,
        et=args.et,
        max_workers=args.max_workers,
        print_output=args.print_output,
        replace_output=args.replace_output
    )
