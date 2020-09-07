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


# 生成用户基本信息关系表
def user_inform_generate(user_num):
    """
        args:
              users_num: 生成用户的个数。
        return:
              user_inform_table: 用户基本信息表（二维列表）[[id,name,age,gender,occupation,register_date]]

        并将[id,name,age,gender,occupation,register_date]写入user_inform.csv

    """
    print("开始生成"+str(user_num) + "个用户基本信息...")
    csvFile = open("data/user_inform.csv", 'w', newline='', encoding='utf-8')
    writer = csv.writer(csvFile)
    user_inform_table = []
    occupation_list = ["teacher", "student", "doctor", "diver", "cleaner", "programmer", "scientist", "artist", "actor"]

    id_len = len(str(user_num))  # 确定id的位数

    a1 = (2000, 1, 1, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组（eg: 1976-01-01 00：00：00）
    a2 = (2020, 7, 7, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组（eg: 1990-12-31 23：59：59）
    start = time.mktime(a1)  # 生成开始时间戳
    end = time.mktime(a2)  # 生成结束时间戳

    user_age = normal_generate(10, 100, 20, 20, user_num)  # 生成满足正态分布的用户年龄，loc = 20，scale = 20

    for i in range(0, user_num):
        user = []
        user_id = ("%0" + str(id_len) + "d") % (i + 1)
        user_name = ''.join(random.sample(string.ascii_lowercase, random.randint(3, 6)))
        user_gender = np.random.randint(0, 2)
        user_occupation = occupation_list[np.random.randint(0, len(occupation_list))]
        t = random.randint(start, end)  # 在开始和结束时间戳中随机取出一个
        date_touple = time.localtime(t)  # 将时间戳生成时间元组
        user_register_date = time.strftime("%Y%m%d", date_touple)  # 将时间元组转成格式化字符串（eg: 2020-7-1）

        user.append(user_id)
        user.append(user_name)
        user.append(user_age[i])
        user.append(user_gender)
        user.append(user_occupation)
        user.append(user_register_date)
        writer.writerow(user)
        user_inform_table.append(user)
    csvFile.close()
    return user_inform_table


# 生成用户登录行为关系表
def user_behavior_generate(user_num, user_behavior_num, user_device_max, user_inform_table):
    """
        args:
              users_num: 用户的个数。
              user_behavior_num: 用户行为记录个数
              user_device_max: 用户持有最大设备数

        生成【id,uid,log_time,ip,device] 并写入user_behavior.csv

        """
    print("开始生成"+str(user_behavior_num)+"条用户登录行为信息...")
    csvFile = open("data/user_behavior.csv", 'w', newline='', encoding='utf-8')
    writer = csv.writer(csvFile)
    uid_len = len(str(user_num))
    id_len = len(str(user_behavior_num+10000))

    for i in range(0, user_behavior_num):
        user_behavior = []
        id = ("%0" + str(id_len) + "d") % (i + 1)
        uid_ = random.randint(1, user_num)
        uid = ("%0" + str(uid_len) + "d") % uid_
        ip = str(random.randint(0, 255)) + "." + str(random.randint(0, 255)) + "." + str(random.randint(0, 255)) + "." \
           + str(random.randint(0, 255))
        device = str(uid) + "device" + str(random.randint(1, user_device_max))

        register_year = parser.parse(str(user_inform_table[uid_-1][5])).year
        register_month = parser.parse(str(user_inform_table[uid_-1][5])).month
        register_day = parser.parse(str(user_inform_table[uid_-1][5])).day

        a1 = (register_year, register_month, register_day, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组（eg: 1976-01-01 00：00：00）
        a2 = (2020, 7, 7, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组（eg: 1990-12-31 23：59：59）

        start = time.mktime(a1)  # 生成开始时间戳
        end = time.mktime(a2)  # 生成结束时间戳
        t = random.randint(start, end)
        date_touple = time.localtime(t)
        log_time = time.strftime("%Y%m%d", date_touple)

        user_behavior.append(id)
        user_behavior.append(uid)
        user_behavior.append(log_time)
        user_behavior.append(ip)
        user_behavior.append(device)
        writer.writerow(user_behavior)
    # 生成插入workload
    r1 = open('workload/temp1_relation', 'a', encoding='utf-8')
    r2 = open('workload/temp2_relation', 'a', encoding='utf-8')
    for i in range(user_behavior_num, user_behavior_num+10000):
        user_behavior = []
        id = ("%0" + str(id_len) + "d") % (i + 1)
        uid_ = random.randint(1, user_num)
        uid = ("%0" + str(uid_len) + "d") % uid_
        ip = str(random.randint(0, 255)) + "." + str(random.randint(0, 255)) + "." + str(random.randint(0, 255)) + "." \
           + str(random.randint(0, 255))
        device = str(uid) + "device" + str(random.randint(1, user_device_max))

        register_year = parser.parse(str(user_inform_table[uid_-1][5])).year
        register_month = parser.parse(str(user_inform_table[uid_-1][5])).month
        register_day = parser.parse(str(user_inform_table[uid_-1][5])).day

        a1 = (register_year, register_month, register_day, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组（eg: 1976-01-01 00：00：00）
        a2 = (2020, 7, 7, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组（eg: 1990-12-31 23：59：59）

        start = time.mktime(a1)  # 生成开始时间戳
        end = time.mktime(a2)  # 生成结束时间戳
        t = random.randint(start, end)
        date_touple = time.localtime(t)
        log_time = time.strftime("%Y%m%d", date_touple)
        sql = "insert into activity values(%s, %s, %s, '%s', '%s')" % (id, uid, log_time, ip, device)
        r1.write(sql + "\n")
        cql = "insert into activity(id, uid, log_time, ip, device) values(%s, %s, '%c%c%c%c-%c%c-%c%c', '%s', '%s')"%(id, uid, log_time[0],
                                                                                                                        log_time[1],log_time[2],log_time[3],
                                                                                                                        log_time[4],log_time[5],log_time[6],
                                                                                                                        log_time[7], ip, device)
        r2.write(cql+"\n")

    r1.close()
    r2.close()
    csvFile.close()


# 生成博文文档模态
def dox_generate(dox_num, user_num, user_inform_table):
    """
        args:
              dox_num : 生成博文文档的个数。
              users_num: 用户的个数。
              user_inform_table: 用户基本信息表。
        return:
              user_blog: 一维列表【”博文id“，”作者id“，”博文话题“】
        并将数据写入blog_dox.json
        """
    print("开始生成" + str(dox_num) + "条博文文档信息...")
    id_len = len(str(dox_num))
    uid_len = len(str(user_num))
    topic_list = ["大学", "开学", "美食", "校园", "旅行", "哈尔滨", "汽车", "农业", "明星", "计算机", "医疗", "美容", "百货",
             "高考", "疫情", "能源", "军事", "毕业", "北京", "河北", "校庆", "美容", "抖音", "武汉", "美国", "互联网"]
    user_blog = []
    with open("data/blog_dox.json", 'w', encoding='utf-8') as f1:
        f1.close()
    csvFile = open("data/temp1.csv", 'w', newline='', encoding='utf-8')
    writer = csv.writer(csvFile)
    for i in range(0, dox_num):
        dic_dox = {}
        user_blog_cell = []

        id = ("%0" + str(id_len) + "d") % (i + 1)
        uid_ = random.randint(1, user_num)
        uid = ("%0" + str(uid_len) + "d") % uid_
        topic = topic_list[int(normal_generate(0, len(topic_list)-1, len(topic_list)/2, len(topic_list), 1)[0])]

        register_year = parser.parse(str(user_inform_table[uid_ - 1][5])).year
        register_month = parser.parse(str(user_inform_table[uid_ - 1][5])).month
        register_day = parser.parse(str(user_inform_table[uid_ - 1][5])).day

        a1 = (register_year, register_month, register_day, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组（eg: 1976-01-01 00：00：00）
        a2 = (2020, 7, 7, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组（eg: 1990-12-31 23：59：59）

        start = time.mktime(a1)  # 生成开始时间戳
        end = time.mktime(a2)  # 生成结束时间戳
        t = random.randint(start, end)
        date_touple = time.localtime(t)
        push_date = time.strftime("%Y%m%d", date_touple)

        blog_title = ''.join(random.sample(string.ascii_lowercase, random.randint(8, 12)))
        blog_keywords = ''.join(random.sample(string.ascii_lowercase, random.randint(8, 12)))
        blog_content = ''.join(choices(string.ascii_letters + string.digits, k=random.randint(300, 4000)))

        dic_dox["id"] = id
        dic_dox["authorid"] = uid
        dic_dox["topic"] = topic
        dic_dox["date"] = push_date
        user_blog_cell.append(id)
        user_blog_cell.append(uid)
        user_blog_cell.append(topic)
        user_blog_cell.append(push_date)
        user_blog.append(user_blog_cell)
        writer.writerow(user_blog_cell)

        blog = {"title": blog_title, "key words": blog_keywords, "content": blog_content}
        dic_dox["blog"] = blog
        with open("data/blog_dox.json", 'a', encoding='utf-8') as f:
            json.dump(dic_dox, f, ensure_ascii=False)
            f.write("\n")
    f.close()
    csvFile.close()
    return user_blog


# 生成用户密码kv数据
def user_password_generate(user_num):
    """
        args:
              users_num: 用户的个数。
        return:
              user_password_kv: 用户密码信息字典{uid:password}
        并将数据写入user_password

        """
    print("为每一位用户生成用户密码...")
    user_password_kv = {}
    uid_len = len(str(user_num))
    for i in range(0, user_num):
        user_id = ("%0" + str(uid_len) + "d") % (i + 1)
        password = ''.join(random.sample(string.ascii_letters, random.randint(6, 11)))
        user_password_kv[user_id] = password
    with open("data/user_password", 'w', encoding='utf-8') as f:
        json.dump(user_password_kv, f, ensure_ascii=False)
    f.close()
    return user_password_kv


# 生成点赞统计kv数据
def user_thumb_generate(user_num, thumb_num, user_inform_table):
    """
        args:
              users_num: 用户的个数。
              thumb_num: 生成点赞个数，注意这里生成的点赞个数而不是数据项个数，一个用户可以有多个点赞对象并把它们保存在列表中作为vlaue。
              user_inform_table: 用户基本信息表。
            return:
              thumb_statistics_kv: 点赞统计字典{ 用户id:【被点赞用户id】}
            并将数据写入：thumb_statistics_kv

            """
    print("生成"+str(thumb_num)+"条点赞数据...")
    thumb_statistics_kv = {}
    uid_len = len(str(user_num))

    a = normal_generate(1, user_num, user_num / 2, user_num, user_num * 2)
    for i in range(0, len(a)):
        uid1_ = random.randint(1, user_num)
        uid1 = ("%0" + str(uid_len) + "d") % uid1_
        uid2_ = int(a[i])
        uid2 = ("%0" + str(uid_len) + "d") % uid2_

        if uid1 in thumb_statistics_kv.keys():
            thumb_statistics_kv[uid1].append(uid2)
        else:
            newv = [uid2]
            thumb_statistics_kv[uid1] = newv
    with open("data/thumb_statistics_kv", 'w', encoding='utf-8') as f:
        json.dump(thumb_statistics_kv, f, ensure_ascii=False)
    f.close()

    # 生成insert工作负载KV
    f1 = open('workload/temp_kv', 'w', encoding='utf-8')
    f1.close()
    f = open('workload/temp_kv', 'a', encoding='utf-8')
    a = normal_generate(1, user_num, user_num / 2, user_num, 10000)
    for i in range(0, len(a)):
        uid1_ = random.randint(1, user_num)
        uid1 = ("%0" + str(uid_len) + "d") % uid1_
        uid2_ = int(a[i])
        uid2 = ("%0" + str(uid_len) + "d") % uid2_
        sql = "insert into userlike values(%s, %s)" % (str(uid1), str(uid2))
        f.write(sql+"\n")
    f.close()
    return thumb_statistics_kv


# 生成大图数据
def graph_generate(user_num, user_inform_table, dox_list):
    """
        args:
              users_num: 用户的个数。
              user_inform_table: 用户基本信息表。
              dox_list: 一维列表【”博文id“，”作者id“，”博文话题“】
        将数据写入：blog_graph.csv
            """
    uid_len = len(str(user_num))
    Triplet = []
    csvFile = open("data/blog_graph.csv", 'w', newline='', encoding='utf-8')
    writer = csv.writer(csvFile)

    # （用户，关注，用户）
    print("（用户，follow，用户）")
    V_num = 50  # 微博大V数量
    V_follow = int(users_num/2)
    Weibo_big_V = []
    csvFileV = open("data/temp2.csv", 'w', newline='', encoding='utf-8')
    writerV = csv.writer(csvFileV)
    cc = random.sample(range(1, users_num+1), V_num)
    for i in range(0, len(cc)):
        uid = ("%0" + str(uid_len) + "d") % int(cc[i])
        Weibo_big_V.append(uid)

    writerV.writerow(Weibo_big_V)
    csvFileV.close()
    for i in range(0, len(Weibo_big_V)):
        uid2 = Weibo_big_V[i]
        for j in range(0, V_follow):
            concern = []
            uid1_ = random.randint(1, user_num)
            uid1 = ("%0" + str(uid_len) + "d") % uid1_
            if uid1 == uid2:
                continue
            concern.append(uid1)
            concern.append("follow")
            concern.append(uid2)
            writer.writerow(concern)

    a = normal_generate(1, user_num, user_num / 2, user_num, user_num * 2)
    con = 0
    while(1):
        concern = []
        uid1_ = random.randint(1, user_num)
        uid1 = ("%0" + str(uid_len) + "d") % uid1_
        uid2_ = int(a[con])
        uid2 = ("%0" + str(uid_len) + "d") % uid2_
        if uid1 == uid2:
            continue
        concern.append(uid1)
        concern.append("follow")
        concern.append(uid2)
        # Triplet.append(concern)
        writer.writerow(concern)
        if con == len(a) - 1:
            break
        con += 1
    # （用户，职业，职业名称）
    print("（用户，position，职业名称）")
    occupation_entity = []
    for i in range(0, len(user_inform_table)):
        occupation_relation = []
        # if user_inform_table[i][4] not in occupation_entity:
        #     occupation_entity.append(user_inform_table[i][4])
        uid = ("%0" + str(uid_len) + "d") % (i + 1)
        occupation_relation.append(uid)
        occupation_relation.append("position")
        occupation_relation.append(user_inform_table[i][4])
        writer.writerow(occupation_relation)
        # Triplet.append(occupation_relation)

    # （用户，用户类型，高级用户/普通用户）
    # 引入高级用户以及普通用户属性 高级用户个数为 senior_num（senior_num < user_num）
    print("（用户，type，senior user/common user）")
    senior_num = user_num / 4
    senior_users = []
    i = 0
    while (1):
        senior_relation = []
        uid_ = random.randint(1, user_num)
        uid = ("%0" + str(uid_len) + "d") % uid_
        if uid in senior_users:
            continue
        senior_users.append(uid)
        i = i + 1
        senior_relation.append(uid)
        senior_relation.append("type")
        senior_relation.append("senior user")
        # Triplet.append(senior_relation)
        writer.writerow(senior_relation)
        if i == senior_num:
            break

    common_users = []
    for i in range(0, user_num):
        common_relation = []
        uid = ("%0" + str(uid_len) + "d") % (i + 1)
        if uid not in senior_users:
            common_users.append(uid)
            common_relation.append(uid)
            common_relation.append("type")
            common_relation.append("common user")
            # Triplet.append(common_relation)
            writer.writerow(common_relation)

    # （用户，性别，男/女）
    print("（用户，gender，male/female）")
    for i in range(0, user_num):
        gender_relation = []
        uid = ("%0" + str(uid_len) + "d") % (i + 1)
        gender_relation.append(uid)
        gender_relation.append("gender")
        if str(user_inform_table[i][3]) == "1":
            gender_relation.append("female")
        else:
            gender_relation.append("male")
        # Triplet.append(gender_relation)
        writer.writerow(gender_relation)

    # （用户，名字，该用户名字）
    print("（用户，name，该用户名字）")
    for i in range(0, user_num):
        name_relation = []
        uid = ("%0" + str(uid_len) + "d") % (i + 1)
        name_relation.append(uid)
        name_relation.append("name")
        name_relation.append(user_inform_table[i][1])
        writer.writerow(name_relation)
        # Triplet.append(name_relation)

    # （用户，发布，博文id）
    print("（用户，publish，博文id）")
    for i in range(0, len(dox_list)):
        push_blog = [dox_list[i][1], "publish", dox_list[i][0]]
        writer.writerow(push_blog)

    # （博文，话题，话题名称）
    print("（博文，topic，话题名称）")
    for i in range(0, len(dox_list)):
        blog_topic = [dox_list[i][0], "topic", dox_list[i][2]]
        writer.writerow(blog_topic)

    # （用户，评论，博文id）
    # 评论总数为 comment_num
    print("（用户，comment，博文id）")
    comment_num = len(dox_list)
    blog_list = normal_generate(0, len(dox_list)-1, len(dox_list)/2, len(dox_list), comment_num)
    for i in range(0, len(blog_list)):
        blog_comment = []
        uid_ = random.randint(1, user_num)
        uid = ("%0" + str(uid_len) + "d") % uid_
        blog_comment.append(uid)
        blog_comment.append("comment")
        blog_comment.append(dox_list[blog_list[i]][0])
        writer.writerow(blog_comment)


if __name__ == '__main__':
    np.random.seed(42)
    random.seed(10)
    factor = 1
    users_num = 10000 * factor  # 生成的用户数量
    users_inform_table = user_inform_generate(users_num)  # 生成用户基本信息
    user_behavior_generate(users_num, 100000 * factor, 3, users_inform_table)  # 生成1000000条用户行为记录
    dox_l = dox_generate(100000 * factor, users_num, users_inform_table)  # 生成博文
    user_password_generate(users_num)  # 生成密码
    user_thumb_generate(users_num, 100000 * factor, users_inform_table)  # 生成点赞统计
    graph_generate(users_num, users_inform_table, dox_l)  # 生成大图数据