- 2020-09-07更新
    关系修改工作负载的sql语句，并增添了cql语句，分别存储在workload_default.txt workload_intelligence_sql.txt workload_intelligence_cql.txt workload_artificial.txt。
- 2020-09-07更新
    按照图数据库对query复杂性的要求，修改了数据生成中的关于图的前两个模态，并在主函数中增加了factor变量，当factor=1时，图模态的数据量为60w条左右，此外更新了数据和负载生成的随机种子设置。

## 目录结构      
Readme.md                   // help      
generation                  // 各模态数据生成代码       
workload                    // 各模态工作负载生成代码      
data：                      // 代码生成数据文件夹      
blog_dox.json               // 文档模态数据        
blog_graph.csv              // 图模态三元组数据        
thumb_statistics_kv         // KV模态点赞统计表      
user_behavior.csv           // 关系模态用户行为表      
user_inform.csv            // 关系模态用户基本信息表       
user_password              // KV模态用户密码表        
workload：                   //代码生成工作负载文件夹       
workload(1~6)           //多模态查询文件标号1~6查询        
workload(1~6)single     //多模态查询文件标号1~6查询的单模态分解       
workload_dox            //文档模态工作负载        
workload_graph          //图模态工作负载        
workload_relation       //关系模态工作负载        
workload_kv             //kv模态工作负载        
注意：workload5 和 workload5a分别对应文档中标号为5的查询生成的两套工作负载同分布且在发布时间字段满足正态分布。     
     代码中的temp文件不用理会
