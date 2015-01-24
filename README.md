filewatcher
===========

###背景
有一些服务部署在欧美机房,不太可能通过pipeline的方式实时同步到中国区统一处理.
一方面是因为到中国的网络不太稳定,另一方面这个延时也很大,单条记录去写性能不高.    
所以目前我们的策略是在前端服务器写入日志(按分钟)后,每隔N分钟通过async的方式批量同步一次,然后在写入到rocketmq. 
后端storm等应用再通过读取rocketmq中的数据来实现准实时的分析.

这个项目就是为了把async后的目录下的文件内容写入到rocketmq/hdfs中(支持多线程,断点续传).

###编译
`mvn assembly:assembly`


###运行命令
`nohup java -Xmx1g -cp filewatcher-1.0-SNAPSHOT.jar me.zhenchuan.files.App --meta_path=/tmp/meta --work_path=/data/aws --file_pattern=.*unbid.log --name=unbid --worker=me.zhenchuan.files.ext.StatsWorker --acceptModifyTime=20150116165000 --parallelism=10 & --property=/tmp/x.properties`


####参数说明
`name` 当前应用的标示,用于生产元数据,监控信息时的前缀信息.

`meta_path`  用于存放元数据信息,目前包含两部分信息:   
1. 当前已经处理的文件列表   2. 正在处理的文件的checkpoint(用于断点续传)       

`work_path`  要处理的文件所在的目录,目前默认会遍历该目录下的所有文件

`file_pattern` 文件名称,支持regex格式

`acceptModifyTime` yyyyMMddHHmmss格式      
1. 设定,则表示只处理文件的修改日期大于当前值的文件.    2. 未设定,则会丢弃以前的文件,从当前时间开始.       

`worker` 具体的worker实现类,目前支持输出到console,rocketmq.   也可以通过继承Worker来实现自己的扩展

`parallelism` worker并行的数量,默认为4

`frequency` 保存checkpoint的间隔时间,默认为1 ,默认单位: 秒

`properties` 提供给worker的properties文件,这些配置会被写入到System.properties


###支持HDFS
`java -Xmx1g -cp filewatcher-1.0-SNAPSHOT.jar:/data/hadoop_config me.zhenchuan.files.hdfs.App --property=hdfs.properties`

####hdfs.properties
```
##应用名称
name=unibd
##hdfs的文件路径,支持时间表达式嵌入      
hdfs_pattern='/user/zhenchuan.liu/tmp/logs/'yyyy/MM/dd/HH/      
##要处理的文件所在的目录       
work_path=/data/aws           
##要处理的文件格式                  
file_pattern=yyyyMMddHH'.*.unbid.log'            
##目前有 HOUR | DAY 两个可选,表示要处理的文件的间隔             
gran=hour                           
##上传到hdfs的文件大小,超过这个可能会被截断          
max_upload_size=200000000             
##生成临时文件的目录          
tmp_dir=/tmp/ 
##安全检查间隔,同gran单位
safe_interval=10
```

####监控hdfs的文件上传信息
`http://192.168.144.200:3456/history?app=aws-unbid`


###TODO 
1. 目前配置太多,把上面opt的参数,都放到一个properties文件中,见 resources/conf.properties.
2. 支持file的rotate方式.



