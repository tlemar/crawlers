import pymysql
import pymysql.cursors as cursors
import unittest
import copy

connectConfig = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'db': 'bigdata',
    'charset': 'utf8mb4',
    'cursorclass': cursors.DictCursor
}


def get_db_connections(config=connectConfig):
    """
    创建一个新的数据库连接，并返回
    :type config:默认使用给定的配置，也可以输入特别的配置
    :return:
    """
    return pymysql.connect(**config)


def get_default_config():
    """
    获取db模块的默认数据库配置文件，可以方便获取后修改.
    返回的是配置本身的引用，还是这个配置复制类？

    使用copy模块的浅拷贝函数copy就可以解决默认配置文件被修改的问题。
    Python 的对象之间的赋值是按引用出传递的
    :return:
    """
    return copy.copy(connectConfig)


default_conns = get_db_connections()


def get_user_list(database, limit="all", conns = default_conns ):
    """
    输入机型名称，
    :return:
    """
    imei_list = []
    if limit == "all":
        query = "select distinct imei from " + database
    elif type(limit) == int:
        query = "select distinct imei from " + database + " limit " + str(limit)
    else:
        print("the input limit is not avaliable, please check", limit)
        return None

    try:
        with conns.cursor() as cursor:
            temp = cursor.execute(query)
            result = cursor.fetchmany(temp)
            for temp in result:
                imei_list.append(temp["imei"])
    except Exception as ex:
        print("error information,  please check ", ex)
        return None
    if 111111111111111 in imei_list:
        imei_list.remove(111111111111111)
    if 123456789012345 in imei_list:
        imei_list.remove(123456789012345)
    return imei_list


def raw_query(query, conns = default_conns, type = "query"):
    """
    封装原始的查询
    :param query:
    :return:
    """
    with conns.cursor() as cursor:
        temp = cursor.execute(query)
        if type is "insert":
            result = conns.commit()
        elif type is "query":
            result = cursor.fetchmany(temp)
        return result


def get_package_list(database, limit="all", conns = default_conns):
    """
    获取所有的package列表
    :param database:
    :param limit:
    :return:
    """
    if limit == "all":
        query = "select distinct package from " + database
    else:
        query = "select distinct package from " + database + " limit " + str(limit)

    package_list = []
    try:
        # print(query)
        with conns.cursor() as cursor:
            temp = cursor.execute(query)
            list_dict = cursor.fetchmany(temp)
            for package in list_dict:
                package_list.append(package["package"])
    except Exception as ex:
        print("error info:", ex)
        return None
    return package_list


def write_file_datebase(file_name, database, batch = 1000):
    """
    从配置的文件中读取数据，写入到指定的database中
    :param file_name:
    :param database:
    :return:
    """
    print(file_name, database)
    db_conf = get_default_config()
    db_conns = get_db_connections(db_conf)
    error_count = 0
    patch_count = 0
    with db_conns.cursor() as cursor:
        record_list = []
        with open(file_name, "r", encoding="utf-8") as f:
            for line in f:
                line_split = line.rstrip("\n").split("\t")
                # print(line_split)

                date = line_split[0][0:10]
                time = line_split[0][11:]
                imei = line_split[1]
                package = line_split[2]
                if len(package) > 200 or len(package)< 2:
                    # print("package name is too long or to short , pass this record", package)
                    error_count += 1
                    continue
                duration = line_split[4]
                if len(duration) < 2:
                    error_count += 1
                    continue
                record_list.append((imei, time,date, package, duration))
                if len(record_list) > batch:
                    print(patch_count)
                    patch_count += 1
                    record_list_str = str(record_list)
                    insert_sql = "insert into " + database + "(imei, t_time, d_date ,package,duration) values " + \
                                 record_list_str[1:len(record_list_str) - 1]
                    # print(insert_sql)
                    record_list.clear()
                    cursor.execute(insert_sql)
                    db_conns.commit()

        # 将没能凑够一次batch的数据输入到数据库中
        record_list_str = str(record_list)
        insert_sql = "insert into " + database + "(imei, t_time,package,duration) values " + \
                     record_list_str[1:len(record_list_str) - 1]
        record_list.clear()
        cursor.execute(insert_sql)
        db_conns.commit()



class DbTestCase(unittest.TestCase):
    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_defalut_connect(self):
        pass

    def test_defalut_config(self):
        default_config = get_default_config()
        self.assertEqual(default_config, connectConfig, '判断与当前连接是否相同')


if __name__ == "main":
    unittest.main()
