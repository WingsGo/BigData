##三大核心组件
HDFS:分布式文件系统
MAPREDUCE:分布式运算编程框架
YARN:分布式资源调度平台

###HDFS
    hadoop具有web可视化功能
    一个namenode(记录存放信息--元数据) + 多个datanode(实际存放位置)
    hdfs namenode -format   #初始化元数据目录
    hadoop fs -ls /     #查看hdfs根目录下文件,分布在多台机器中
    hadoop fs 


HDFS为用户提供一个统一的目录树
 1. 为了解决文件大小限制，使用了多机存储，切块后存储在多台datanode中
 2. 为了保障可靠性，使用多副本备份
 3. 文件块的存储信息记录在namenode中

控制集群时原理
ssh免密登录,在namenode的主机上使用shell脚本,免密登录,执行脚本运行hadoop

 1. 配置master到集群中所有机器的免密登录
 2. 执行ssh 0.0.0.0
 3. 修改hadoop安装目录中/etc/hadoop/slaves,把需要启动datanode进程的节点列入(ip地址)
 4. 在master中使用脚本 start-dfs.sh启动整个集群
 5. 使用stop-dfs.sh停止整个集群
 
###Namenode-checkpoint机制
 1. namenode会把实时的完整的元数据存储在内存中
 2. namenode会定期在磁盘中存储内存元数据在某个时间点上的镜像文件(fsimage_0000000000000000349)
 3. namenode会把引起元数据变化的客户端操作记录在edits日志文件中
 
secondarynamenode会定期从namenode上下载fsimage镜像和新生成的edits日志，然后加载fsimage镜像到内存中，**然后顺序解析edits文件**，对内存中的元数据对象进行修改（整合）
整合完成后，将内存元数据序列化成一个新的fsimage，并将这个fsimage镜像文件上传给namenode
![checkpoint](https://leanote.com/api/file/getImage?fileId=5d4e4c95ab64416e13001390)

###虚拟机网络
![图片标题](https://leanote.com/api/file/getImage?fileId=5d4e4df9ab64416e1300139c)

###HDFS工作机制
![图片标题](https://leanote.com/api/file/getImage?fileId=5d4e4e1fab64417018001396)

###客户端写数据到HDFS的流程
![客户端写数据到HDFS的流程](https://leanote.com/api/file/getImage?fileId=5d4e5880ab64417018001406)

###客户端从HDFS中读数据的流程
![客户端从HDFS中读数据的流程](https://leanote.com/api/file/getImage?fileId=5d4e58b8ab64417018001409)


