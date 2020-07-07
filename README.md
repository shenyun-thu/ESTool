Elasticsearch tool
==================

基本用法：（其中有默认值的参数为可选项 无默认值的参数必须指定 ）
```
usage: java -jar aggregation.jar
  -cluster <arg>   es集群名称（默认为elasticsearch）
  -config <arg>    指定配置文件路径
  -field <arg>     指定需要从es中下载信息包含的字段（默认为全量下载）多个字段逗号隔开
  -file <arg>      指定输出文件路径(默认在当前路径 文件名由 index+type构成)
  -help            打印帮助信息
  -index <arg>     es中数据存储的索引
  -ip <arg>        es集群任意节点的地址
  -port <arg>      es集群client模式通信端口（默认为9300）
  -type <arg>      es中数据存储的类型
 ```
 

 
配置文件样例：
```
_start_at:1535073771,1535073773
_sport:21000
_src_ip:218.1.106.42,218.1.106.43
```
配置文件说明：
```
基本格式为  <field>:<value>
value为逗号分隔两个值时表示用这两个值为下界和上界过滤，其中上下界均包含
value 为单独值时表示用该值进行过滤
```
