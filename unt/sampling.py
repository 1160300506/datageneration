import random


# 六个模板分别抽取:
#   11
#   2
#   2
#   15
#   15
#   15


def sample(num, total_line):
    h = set()
    while len(h) < num:
        h.add(random.randint(1, total_line))
    return h


def extract1(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[67]
            variable2 = line.split(" ")[79]
            graph_sql = "Select a.s from graph a join graph b on a.s = b.s join graph c on b.o = c.s " \
                        "join graph d on a.s = d.s " \
                        "where a.p = 'follow' and a.o = %s and b.p = 'publish' and c.p = 'topic' and c.o = %s " \
                        "and d.p = 'gender' and d.o = 'female';" % (variable1, variable2)

            relation_sql = "Select activity.device, count(activity.device) as device_count from activity where" \
                           " activity.log_time between %s and %s and basicinfo.id in (graph_answer.id)" \
                           " group by device;" % ("20200701", "20200707")
            f_sampling_graph.write(graph_sql+"\n")
            f_sampling_relation.write(relation_sql+"\n")
    f.close()


def extract2(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[33]
            variable2 = line.split(" ")[37]
            dox_sql = "Select document.id from document where document.date between 20200607 and 20200707;"
            relation_sql = "Select basicinfo.id from basicinfo join activity on basicinfo.id = activity.uid" \
                                       " where basicinfo.register_date < %s and basicinfo.id in (graph_answer.id) and" \
                                       " activity.log_time = %s;" % (variable1, variable2)
            kv_sql = "Select * from userlike where userlike.uid in (relation_answer.id);"
            f_sampling_dox.write(dox_sql+"\n")
            f_sampling_relation.write(relation_sql+"\n")
            f_sampling_kv.write(kv_sql+"\n")

    f.close()


def extract3(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[33]
            dox_sql = "Select document.id from document where document.date between 20200607 and 20200707;"
            relation_sql = "Select basicinfo.id from basicinfo join activity on basicinfo.id = activity.uid" \
                           " where basicinfo.register_date < %s and basicinfo.id in (document_answer.id) and" \
                           " activity.log_time >= 20200701;" % variable1
            kv_sql = "Select * from userlike where userlike.uid in (relation_answer.id);"
            f_sampling_dox.write(dox_sql+"\n")
            f_sampling_relation.write(relation_sql+"\n")
            f_sampling_kv.write(kv_sql+"\n")
    f.close()


def extract4(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[49]
            variable2 = line.split(" ")[57]
            variable3 = line.split(" ")[64] + " " + line.split(" ")[65]
            variable4 = line.split(" ")[69][:-1]
            graph_sql = "Select a.s from graph a join graph b on a.s = b.s join graph c on" \
                        " a.s = c.s where a.p = 'follow' and a.o = %s " \
                        "and b.p = 'position' and b.o = %s and c.p = 'type' " \
                        "and c.o = %s;" % (variable1, variable2, variable3)
            relation_sql = "Select * from basicinfo where " \
                           "basicinfo.id in (graph_answer.id) and basicinfo.register_date < %s;" % variable4
            f_sampling_graph.write(graph_sql + "\n")
            f_sampling_relation.write(relation_sql + "\n")
    f.close()


def extract5(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[25]
            dox_sql = "Select document.id from document where document.date = %s;" % variable1
            relation_sql = "select basicinfo.id from basicinfo" \
                           " where basicinfo.age between 20 and 30 and basicinfo.id in (document_answer.id);"
            kv_sql = "Select * from userlike where userlike.uid in (relation_answer.id);"
            f_sampling_dox.write(dox_sql+"\n")
            f_sampling_relation.write(relation_sql+"\n")
            f_sampling_kv.write(kv_sql+"\n")

    f.close()


def extract6(filename, h):
    f = open(filename, encoding='utf-8')
    c = 0
    for line in f:
        c = c + 1
        if c in h:
            f_sampling.write(line)
            variable1 = line.split(" ")[60]
            graph_sql = "Select a.s from graph a join graph b on a.s = b.s join graph c on b.o = c.s" \
                      " where a.p = 'type' and a.o = 'senior user' and b.p = 'comment'" \
                      " and c.p = 'topic' and c.o = %s;" % variable1
            kv_sql = "Select * from userlike where userlike.uid in (relation_answer.id);"
            f_sampling_graph.write(graph_sql + "\n")
            f_sampling_kv.write(kv_sql + "\n")

    f.close()




if __name__ == '__main__':

    for i in range(1, 7):
        random.seed(i)
        f_sampling = open('sampling/sampling_multi' + str(i), 'a', encoding='utf-8')
        f_sampling_relation = open('sampling/sampling_multi' + str(i) + "_relation", 'a', encoding='utf-8')
        f_sampling_dox = open('sampling/sampling_multi' + str(i) + "_dox", 'a', encoding='utf-8')
        f_sampling_graph = open('sampling/sampling_multi' + str(i) + "_graph", 'a', encoding='utf-8')
        f_sampling_kv = open('sampling/sampling_multi' + str(i) + "_kv", 'a', encoding='utf-8')
        extract1("workload/workload1", sample(11, 100))
        extract2("workload/workload2", sample(2, 5))
        extract3("workload/workload3", sample(2, 5))
        extract4("workload/workload4", sample(15, 10000))
        extract5("workload/workload5", sample(15, 10000))
        extract6("workload/workload6", sample(15, 10000))
        f_sampling.close()