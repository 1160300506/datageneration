import numpy as np
import random
import string
import time
import dateutil.parser as parser
from random import choices
import json
import csv


# 生成指定区间（start，end）的满足正太分布 loc scale 数量为size的随机数
def normal_generate(start, end, loc, scale, size):
    normal_list = []
    while (1):
        value = int(np.random.normal(loc, scale, 1)[0])
        if value < start or value > end:
            continue
        normal_list.append(value)
        if len(normal_list) == size:
            break
    return normal_list


# 读取用户基本信息
def read_user_inform():
    with open('data/user_inform.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        user_inform_table = list(reader)
    f.close()
    return user_inform_table


# 读取用户行为信息
def read_user_behavior():
    with open('data/user_behavior.csv', 'r') as f:
        reader = csv.reader(f)
        user_behavior_table = list(reader)
    f.close()
    return user_behavior_table


# 读取用户发表博文话题关系
def read_user_blog():
    with open('data/temp1.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        user_blog = list(reader)
    f.close()
    return user_blog


# 读取用户的关注关系以及评论关系
def read_follow_comment():
    follow = []
    comment = []
    with open('data/blog_graph.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[1] == "follow":
                follow.append(row)
            if row[1] == "comment":
                comment.append(row)
    return follow, comment


# 读取微博大V用户列表
def read_blog_v():
    with open('data/temp2.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        blog_v = list(reader)
    f.close()
    return blog_v[0]


# 获取高级用户
def read_senior_user():
    senior_user = []
    with open('data/blog_graph.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[1] == "type" and row[2] == "senior user":
                senior_user.append(row[0])
    f.close()
    return senior_user


# 查询关注了某大V的发表过‘手机’话题博文的女性用户在过去一周的登录设备统计。

def workload1(mode=0):
    workload_num = 100
    topic_list = ["大学", "开学", "美食", "校园", "旅行", "哈尔滨", "汽车", "农业", "明星", "计算机", "医疗", "美容", "百货",
                  "高考", "疫情", "能源", "军事", "毕业", "北京", "河北", "校庆", "美容", "抖音", "武汉", "美国", "互联网"]
    user_inform_table = read_user_inform()
    user_behavior_table = read_user_behavior()
    dox_l = read_user_blog()
    follow, comment = read_follow_comment()
    blog_v = read_blog_v()
    f = open('workload/workload1', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload1', 'a', encoding='utf-8')
    f_single = open('workload/workload1_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload1_single', 'a', encoding='utf-8')
    f_graph = open('workload/workload_graph', 'a', encoding='utf-8')

    f_relation_default = open('workload/workload_default.txt', 'a', encoding='utf-8')
    f_relation_intelligence_sql = open('workload/workload_intelligence_sql.txt', 'a', encoding='utf-8')
    f_relation_intelligence_cql = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_artificial = open('workload/workload_artificial.txt', 'a', encoding='utf-8')

    count = 0
    for i in range(0, len(topic_list)):
        for j in range(0, len(blog_v)):
            count += 1
            sql = "Select activity.device, count(activity.device) as device_count where log_time %s" \
                  "and %s and uid in (Select gra.s, gra.eo, gra.fo join gra (s = a.s, o = a.o, p = a.p, bp = b.p, " \
                  "bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o, ep = e.p, eo = e.o, fp = f.p, fo = f.o) " \
                  "a, b, c, d, e, f a.s = b.s and b.o = c.s and a.s = d.s and a.s = e.s and a.s = f.s where " \
                  "gra.p = 'follow' and gra.o = '%s' and gra.bp = 'publish' and gra.cp = 'topic' and gra.co" \
                  "= '%s' and gra.dp = 'gender' and gra.do = 'female' and gra.ep = 'occupation' and gra.fp = 'name') " \
                  "group by device;" % ("20200701", "20200707", blog_v[j], topic_list[i])
            f.write(sql + "\n")

            sql_graph = "Select a.s, e.o, f.o from graph a join graph b on a.s = b.s join graph c on b.o = c.s " \
                        "join graph d on a.s = d.s join graph e on a.s = e.s join graph f on a.s = f.s " \
                        "where a.p = 'follow' and a.o = '%s' and b.p = 'publish' and c.p = 'topic' and c.o = '%s' " \
                        "and d.p = 'gender' and d.o = 'female' and e.p = 'occupation' and f.p = 'name';" % (
                            blog_v[j], topic_list[i])

            f_graph.write(sql_graph + "\n")
            f_single.write(sql_graph + "\n")
            sql_relation = ""
            if mode == 0:
                sql_relation = "Select activity.device, count(activity.device) as device_count from activity where" \
                           " activity.log_time between %s and %s and uid >= 101 and uid <= 105" \
                           " group by device;" % ("20200701", "20200707")
                f_relation_default.write(sql_relation + "\n")
            if mode == 1:
                cql_relation = "Select device from activity where" \
                               " log_time >= '%s' and log_time <= '%s' and uid >= 101 and uid <= 105" \
                               " allow filtering;" % ("2020-07-01", "2020-07-07")
                f_relation_intelligence_cql.write(cql_relation + "\n")
            if mode == 2:
                cql_relation = "Select device from activity where" \
                               " log_time >= '%s' and log_time <= '%s' and uid >= 101 and uid <= 105" \
                               " allow filtering;" % ("'2020-07-01'", "'2020-07-07'")
                f_relation_artificial.write(cql_relation + "\n")
            f_single.write(sql_relation + "\n")
            if count == workload_num:
                f.close()
                f_single.close()
                f_relation_default.close()
                f_relation_intelligence_sql.close()
                f_relation_intelligence_cql.close()
                f_relation_artificial.close()
                f_graph.close()
                return

    ## 100x查询关注了某大V的发表过‘手机’话题帖子的女性用户在过去一周的登录设备统计。
    # uid = []
    # user_inform_table = read_user_inform()
    # dox_l = read_user_blog()
    # user_behavior_table = read_user_behavior()
    # follow, comment = read_follow_comment()
    # blog_v = read_blog_v()
    # print("在发表某一话题的用户")
    # for i in range(0, len(dox_l)):
    #     if dox_l[i][2] == "大学":
    #         uid.append(dox_l[i][1])
    # print(len(uid))
    # uidnew1 = []
    # print("2. 在女性用户中")
    # for i in range(0, len(uid)):
    #     if user_inform_table[int(uid[i])][3] == "1":
    #         uidnew1.append(uid[i])
    # uid = []
    # print(len(uidnew1))
    #
    # print("3. 在过去一周")
    # uidnew2 = []
    # for i in range(0, len(user_behavior_table)):
    #     if user_behavior_table[i][1] in uidnew1 and 20200707 >= int(user_behavior_table[i][2]) >= 20200701:
    #         uidnew2.append(user_behavior_table[i][1])
    #
    # flag = 0
    # for i in range(0, len(uidnew2)):
    #     for j in range(0, len(follow)):
    #         if follow[j][0] == uidnew2[i] and follow[j][2] == blog_v[0]:
    #             flag = 1
    #             break
    #     if flag == 1:
    #         break
    # print(flag)


# (5x)查询所有注册日期早于2012年的昨日活跃用户且在最近一个月发表过博文的用户的点赞情况
def workload2(mode=0):
    workload_num = 5
    user_inform_table = read_user_inform()
    user_behavior_table = read_user_behavior()
    dox_l = read_user_blog()
    f = open('workload/workload2', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload2', 'a', encoding='utf-8')
    count = 0
    f_single = open('workload/workload2_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload2_single', 'a', encoding='utf-8')
    f_dox = open('workload/workload_dox', 'a', encoding='utf-8')
    f_relation_default = open('workload/workload_default.txt', 'a', encoding='utf-8')
    f_relation_intelligence_sql = open('workload/workload_intelligence_sql.txt', 'a', encoding='utf-8')
    f_relation_intelligence_cql = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_artificial = open('workload/workload_artificial.txt', 'a', encoding='utf-8')
    f_kv = open('workload/workload_kv', 'a', encoding='utf-8')

    for i in range(2001, 2021):
        data = []
        uid = []
        for j in range(0, len(user_inform_table)):
            if int(user_inform_table[j][5]) < i * 10000:
                uid.append(user_inform_table[j][0])

        recent_uid = []
        for j in range(0, len(dox_l)):
            if 20200707 >= int(dox_l[j][3]) >= 20200607:
                recent_uid.append(dox_l[j][1])
        uid_new = list(set(recent_uid) & set(uid))

        for j in range(0, len(user_behavior_table)):
            if user_behavior_table[j][1] in uid_new:
                if user_behavior_table[j][2] not in data:
                    data.append(user_behavior_table[j][2])
                    mysql = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id" \
                            " Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id " \
                            "= r2.id where register_date < %s0101 and log_time = %s, Select document.id where" \
                            " document.date between 20200607 and 20200707 rel.id = document.id )" % (
                                str(i), user_behavior_table[j][2])
                    f.write(mysql + "\n")
                    mysql_dox = "Select document.id from document where document.date between 20200607 and 20200707;"
                    sql_relation = ""
                    if mode == 0:
                        sql_relation = "Select basicinfo.id from basicinfo join activity on basicinfo.id = activity.uid" \
                                       " where basicinfo.register_date < %s0101 and uid >= 101 and uid <= 105" \
                                       " activity.log_time = %s;" % (str(i), user_behavior_table[j][2])
                        f_relation_default.write(sql_relation + "\n")
                    if mode == 1:
                        sql_relation = "select id from basicinfo where id >= 101 and id <= 105" \
                                       " and register_date < %s0101;" %str(i)
                        cql_relation = "select uid from activity where uid >= 101 and uid <= 105" \
                                       " and log_time = '%c%c%c%c-%c%c-%c%c' allow filtering;" % (user_behavior_table[j][2][0], user_behavior_table[j][2][1],
                                                                               user_behavior_table[j][2][2], user_behavior_table[j][2][3],
                                                                               user_behavior_table[j][2][4], user_behavior_table[j][2][5],
                                                                               user_behavior_table[j][2][6], user_behavior_table[j][2][7])
                        f_relation_intelligence_sql.write(sql_relation + "\n")
                        f_relation_intelligence_cql.write(cql_relation + "\n")
                    if mode == 2:
                        cql_relation = "select uid from activity where uid >= 101 and uid <= 105" \
                                       " and log_time = '%c%c%c%c-%c%c-%c%c' allow filtering;" % (
                                       user_behavior_table[j][2][0], user_behavior_table[j][2][1],
                                       user_behavior_table[j][2][2], user_behavior_table[j][2][3],
                                       user_behavior_table[j][2][4], user_behavior_table[j][2][5],
                                       user_behavior_table[j][2][6], user_behavior_table[j][2][7])
                        f_relation_artificial.write(cql_relation + "\n")
                        cql_relation = "select id from basicinfo where id >= 101 and id <= 105 and register_date = '%s-01-01' allow filtering;" %str(i)
                        f_relation_artificial.write(cql_relation + "\n")
                    mysql_kv = "Select * from userlike where userlike.uid in (relation_answer.id);"

                    f_single.write(mysql_dox + "\n" + sql_relation + "\n" + mysql_kv + "\n")
                    f_dox.write(mysql_dox + "\n")
                    f_kv.write(mysql_kv + "\n")
                    count += 1
            if count == workload_num:
                f.close()
                f_relation_default.close()
                f_relation_intelligence_sql.close()
                f_relation_intelligence_cql.close()
                f_relation_artificial.close()
                f_single.close()
                f_dox.close()
                f_kv.close()
                return


#  (5x) 查询所有注册日期早于2012年的本周活跃用户且在最近一年发表过博文的用户的点赞情况
def workload3(mode=0):
    workload_num = 5

    f = open('workload/workload3', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload3', 'a', encoding='utf-8')
    count = 0
    f_kv = open('workload/workload_kv', 'a', encoding='utf-8')
    f_single = open('workload/workload3_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload3_single', 'a', encoding='utf-8')
    f_dox = open('workload/workload_dox', 'a', encoding='utf-8')
    f_relation_default = open('workload/workload_default.txt', 'a', encoding='utf-8')
    f_relation_intelligence_sql = open('workload/workload_intelligence_sql.txt', 'a', encoding='utf-8')
    f_relation_intelligence_cql = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_artificial = open('workload/workload_artificial.txt', 'a', encoding='utf-8')

    for i in range(2012, 2017):
        mysql = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id Join rel" \
                " (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where" \
                " register_date < %s0101 and log_time>= 20200701 , Select document.id where document.date" \
                " 20190707 and 20200707 rel.id = document.id )" % str(i)
        mysql_dox = "Select document.id from document where document.date between 20190707 and 20200707;"
        sql_relation = ""
        if mode == 0:
            sql_relation = "Select basicinfo.id from basicinfo join activity on basicinfo.id = activity.uid" \
                           " where basicinfo.register_date < %s0101 and uid >= 101 and uid <= 105" \
                           " activity.log_time >= 20200701;" % (str(i))
            f_relation_default.write(sql_relation + "\n")
        if mode == 1:
            cql_relation = "select uid from activity where uid >= 101 and uid <= 105;" \
                           " and log_time >= '2020-07-01' allow filtering"
            sql_relation = "select id from basicinfo where id >= 101 and id <= 105;" \
                           " and register_date < %s0101" % str(i)
            f_relation_intelligence_sql.write(sql_relation + "\n")
            f_relation_intelligence_cql.write(cql_relation + "\n")
        if mode == 2:
            cql_relation = "select id from basicinfo where id >= 101 and id <= 105 and register_date = '%s-01-01' allow filtering;" % str(
                i)
            f_relation_artificial.write(cql_relation + "\n")
            cql_relation = "select uid from activity where uid >= 101 and uid <= 105" \
                           " and log_time >= '2020-07-01' allow filtering;"
            f_relation_artificial.write(cql_relation + "\n")
        mysql_kv = "Select * from userlike where userlike.uid in (relation_answer.id);"
        f_dox.write(mysql_dox + "\n")
        f_single.write(sql_relation + "\n" + mysql_dox + "\n" + mysql_kv + "\n")
        f.write(mysql + "\n")
        f_kv.write(mysql_kv + "\n")
    f.close()
    f_single.close()
    f_relation_default.close()
    f_relation_intelligence_sql.close()
    f_relation_intelligence_cql.close()
    f_relation_artificial.close()
    f_dox.close()
    f_kv.close()


def workload4(mode=0):
    workload_num = 10000
    occupation_list = ["teacher", "student", "doctor", "diver", "cleaner", "programmer", "scientist", "artist", "actor"]
    type_user = ["senior user", "common user"]
    count = 0
    f = open('workload/workload4', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload4', 'a', encoding='utf-8')
    blog_v = read_blog_v()

    f_graph = open('workload/workload_graph', 'a', encoding='utf-8')
    f_single = open('workload/workload4_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload4_single', 'a', encoding='utf-8')
    f_relation_default = open('workload/workload_default.txt', 'a', encoding='utf-8')
    f_relation_intelligence_sql = open('workload/workload_intelligence_sql.txt', 'a', encoding='utf-8')
    f_relation_intelligence_cql = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_artificial = open('workload/workload_artificial.txt', 'a', encoding='utf-8')

    while (True):
        for i in range(0, len(blog_v)):
            for j in range(0, len(occupation_list)):
                for k in range(0, len(type_user)):
                    for z in range(2010, 2021):
                        mysql = "Select rel.* where rel.id in (Select gra.s, gra.do join gra (s = a.s, o = a.o, " \
                                "p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o) " \
                                "a, b, c, d a.s = b.s and a.s = c.s and a.s = d.s where " \
                                "gra.p = 'follow' and gra.o = '%s' and gra.bp = 'position' " \
                                "and gra.bo = '%s' gra.cp = 'type' and gra.co = '%s' and gra.dp = 'name') " \
                                "and rel.register_date < %s0101" % (blog_v[i], occupation_list[j], type_user[k], str(z))
                        mysql_graph = "Select a.s, d.o from graph a join graph b on a.s = b.s join graph c on" \
                                      " a.s = c.s join graph d on a.s = d.s where a.p = 'follow' and a.o = '%s' " \
                                      "and b.p = 'position' and b.o = '%s' and c.p = 'type' and c.o = '%s' " \
                                      "and d.p = 'name';" % (blog_v[i], occupation_list[j], type_user[k])
                        sql_relation = ""
                        if mode == 0:
                            sql_relation = "Select * from basicinfo where id " \
                                           ">= 101 and id <= 105 and basicinfo.register_date < %s0101;" % str(z)
                            f_relation_default.write(sql_relation + "\n")
                        if mode == 1:
                            sql_relation = "Select * from basicinfo where id " \
                                           ">= 101 and id <= 105 and basicinfo.register_date < %s0101;" % str(z)
                            f_relation_intelligence_sql.write(sql_relation + "\n")
                        if mode == 2:
                            cql_relation = "select * from basicinfo where id >= 101 and id <= 105" \
                                           " and register_date < '%s-01-01' allow filtering;" % str(z)
                            f_relation_artificial.write(cql_relation + "\n")
                        f_graph.write(mysql_graph + "\n")
                        f_single.write(sql_relation + "\n" + mysql_graph + "\n")
                        f.write(mysql + "\n")
                        count += 1
                        if count == workload_num:
                            f.close()
                            f_single.close()
                            f_relation_default.close()
                            f_relation_intelligence_sql.close()
                            f_relation_intelligence_cql.close()
                            f_relation_artificial.close()
                            f_graph.close()
                            return

    # uid = []
    # follow, comment = read_follow_comment()
    # inform = read_user_inform()
    # a = 0
    # for i in range(0, len(follow)):
    #     if follow[i][2] == "00972":
    #         uid.append(follow[i][0])
    # uidnew = []
    # for i in range(0, len(uid)):
    #     for j in range(0, len(inform)):
    #         if inform[j][0] == uid[i] and inform[j][4] == "teacher":
    #             uidnew.append(uid[i])
    # print(len(uidnew))
    #
    # sen = read_senior_user()
    # uid2 = []
    # for i in range(0, len(uidnew)):
    #     if uidnew[i] in sen:
    #         uid2.append(uidnew[i])
    # print(uid2)
    #


# (10000x)查询年龄在20-30，在20120501发表过博文的所有用户的点赞情况
def workload5(mode=0):
    workload_num = 10000
    f = open('workload/workload5', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload5', 'a', encoding='utf-8')

    f_single = open('workload/workload5_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload5_single', 'a', encoding='utf-8')

    f_relation_default = open('workload/workload_default.txt', 'a', encoding='utf-8')
    f_relation_intelligence_sql = open('workload/workload_intelligence_sql.txt', 'a', encoding='utf-8')
    f_relation_intelligence_cql = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_artificial = open('workload/workload_artificial.txt', 'a', encoding='utf-8')
    f_kv = open('workload/workload_kv', 'a', encoding='utf-8')
    f_dox = open('workload/workload_dox', 'a', encoding='utf-8')

    c = normal_generate(20000101, 20200707, 20100101, 3650, workload_num)
    for i in range(0, len(c)):
        mysql = "Select kv.set where kv.id in (select id Join sameid (id = rel.id) Select rel.id where rel.age " \
                "between 20 and 30, Select document.id where document.date = %s " \
                "rel.id = document.id)" % str(c[i])
        mysql_dox = "Select document.id from document where document.date = %s;" % str(c[i])
        sql_relation = ""
        if mode == 0:
            sql_relation = "select basicinfo.id from basicinfo" \
                           " where basicinfo.age between 20 and 30 and id >= 101 and id <= 105;"
            f_relation_default.write(sql_relation + "\n")
        if mode == 1:
            sql_relation = "select basicinfo.id from basicinfo" \
                           " where basicinfo.age between 20 and 30 and id >= 101 and id <= 105;"
            f_relation_intelligence_sql.write(sql_relation + "\n")
        if mode == 2:
            cql_relation = "select id from basicinfo where age >= 20 and age <= 30" \
                           " and id >= 101 and id <= 105 allow filtering;"
            f_relation_artificial.write(cql_relation + "\n")
        mysql_kv = "Select * from userlike where userlike.uid in relation_answer.id;"
        f_dox.write(mysql_dox + "\n")
        f_kv.write(mysql_kv + "\n")
        f_single.write(mysql_dox + "\n" + sql_relation + "\n" + mysql_kv + "\n")
        f.write(mysql + "\n")
    f.close()
    f_single.close()
    f_kv.close()
    f_dox.close()

    # 再次生成一组同分布的负载
    c = normal_generate(20000101, 20200707, 20100101, 3650, workload_num)
    f = open('workload/workload5a', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload5a', 'a', encoding='utf-8')
    f_single = open('workload/workload5a_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload5a_single', 'a', encoding='utf-8')
    f_relation = open('workload/workload_relation', 'a', encoding='utf-8')
    f_kv = open('workload/workload_kv', 'a', encoding='utf-8')
    f_dox = open('workload/workload_dox', 'a', encoding='utf-8')
    for i in range(0, len(c)):
        mysql = "Select kv.set where kv.id in (select id Join sameid (id = rel.id) Select rel.id where rel.age " \
                "between 20 and 30, Select document.id where document.date = %s " \
                "rel.id = document.id)" % str(c[i])
        mysql_dox = "Select document.id from document where document.date = %s;" % str(c[i])
        sql_relation = ""
        if mode == 0:
            sql_relation = "select basicinfo.id from basicinfo" \
                           " where basicinfo.age between 20 and 30 and id >= 101 and id <= 105;"
            f_relation_default.write(sql_relation + "\n")
        if mode == 1:
            sql_relation = "select basicinfo.id from basicinfo" \
                           " where basicinfo.age between 20 and 30 and id >= 101 and id <= 105;"
            f_relation_intelligence_sql.write(sql_relation + "\n")
        if mode == 2:
            cql_relation = "select id from basicinfo where age >= 20 and age <= 30" \
                           " and id >= 101 and id <= 105 allow filtering;"
            f_relation_artificial.write(cql_relation + "\n")
        mysql_kv = "Select * from userlike where userlike.uid in relation_answer.id;"
        f_dox.write(mysql_dox + "\n")
        f_relation.write(sql_relation + "\n")
        f_kv.write(mysql_kv + "\n")
        f_single.write(mysql_dox + "\n" + sql_relation + "\n" + mysql_kv + "\n")
        f.write(mysql + "\n")
    f.close()
    f_single.close()
    f_kv.close()
    f_relation.close()
    f_relation_default.close()
    f_relation_intelligence_sql.close()
    f_relation_intelligence_cql.close()
    f_relation_artificial.close()
    f_dox.close()


#  (10000x)查询评论过话题为‘大学’的博文的高级用户的点赞情况
def workload6(mode=0):
    workload_num = 10000
    f_graph = open('workload/workload_graph', 'a', encoding='utf-8')
    f_single = open('workload/workload6_single', 'w', encoding='utf-8')
    f_single.close()
    f_single = open('workload/workload6_single', 'a', encoding='utf-8')
    f_kv = open('workload/workload_kv', 'a', encoding='utf-8')

    f = open('workload/workload6', 'w', encoding='utf-8')
    f.close()
    f = open('workload/workload6', 'a', encoding='utf-8')
    topic_list = ["大学", "开学", "美食", "校园", "旅行", "哈尔滨", "汽车", "农业", "明星", "计算机", "医疗", "美容", "百货",
                  "高考", "疫情", "能源", "军事", "毕业", "北京", "河北", "校庆", "美容", "抖音", "武汉", "美国", "互联网"]
    c = normal_generate(0, len(topic_list) - 1, len(topic_list) / 2, len(topic_list), workload_num)
    for i in range(0, len(c)):
        mysql = "Select kv.set where kv.id in (Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o," \
                " cp = c.p, co = c.o) a, b, c a.s = b.s and b.o = c.s where gra.p = 'type' and gra.o = 'senior user'" \
                " and gra.bp = 'comment' and gra.cp = 'topic' and gra.co = '%s')" % str(topic_list[c[i]])
        mysql_graph = "Select a.s from graph a join graph b on a.s = b.s join graph c on b.o = c.s" \
                      " where a.p = 'type' and a.o = 'senior user' and b.p = 'comment'" \
                      " and c.p = 'topic' and c.o = '%s';" % str(topic_list[c[i]])
        mysql_kv = "Select * from userlike where userlike.uid in graph_answer.s;"

        f_graph.write(mysql_graph + "\n")
        f_single.write(mysql_graph + "\n" + mysql_kv + "\n")
        f_kv.write(mysql_kv + "\n")
        f.write(mysql + "\n")
    f.close()
    f_single.close()
    f_graph.close()
    f_kv.close()


if __name__ == '__main__':
    np.random.seed(42)
    random.seed(10)
    f_graph_re = open('workload/workload_graph', 'w', encoding='utf-8')
    f_kv_re = open('workload/workload_kv', 'w', encoding='utf-8')
    f_dox_re = open('workload/workload_dox', 'w', encoding='utf-8')
    f_relation_re = open('workload/workload_relation', 'w', encoding='utf-8')
    f_graph_re.close()
    f_dox_re.close()
    f_kv_re.close()
    f_relation_re.close()

    temp_kv = open('workload/temp_kv', 'r', encoding='utf-8')
    temp1_relation = open('workload/temp1_relation', 'r', encoding='utf-8')
    f_relation_re = open('workload/workload_default.txt', 'a', encoding='utf-8')
    temp2_relation = open('workload/temp2_relation', 'r', encoding='utf-8')
    f_relation_re1 = open('workload/workload_intelligence_cql.txt', 'a', encoding='utf-8')
    f_relation_re2 = open('workload/workload_artificial.txt', 'a', encoding='utf-8')
    f_kv_re = open('workload/workload_kv', 'a', encoding='utf-8')
    for line in temp1_relation:
        f_relation_re.write(line)
    for line in temp2_relation:
        f_relation_re1.write(line)
        f_relation_re2.write(line)
    for line in temp_kv:
        f_kv_re.write(line)
    f_relation_re.close()
    f_relation_re1.close()
    f_relation_re2.close()
    f_kv_re.close()
    temp1_relation.close()
    temp_kv.close()

    workload1(2)
    workload2(2)
    workload3(2)
    workload4(2)
    workload5(2)
    workload6(2)
