# 说明
在一个实际的项目中可能存在多种数据存储技术，如传统的关系数据库，新兴的Hadoop、nosql、搜索引擎等。
进行综合分析时可能需要几类数据之间的关联处理，一种解决方案是把数据合并在一起，如统一抽取到Hadoop进行分析。
第二种方案可以通过数据联邦的技术实现多种异构数据源的关联分析，其优点是不用迁移数据。

HAWQ Extension Framework (PXF)提供了一种灵活、可扩展的框架，使HAWQ能查询外部数据。HAWQ项目本身的PXF内置提供了HADOOP系列存储的插件-HDFS/HIVE/HBASE。

而本项目提供的插件可以实现HAWQ对关系数据库（JDBC）、搜索引擎（Solr）等外部数据的查询。
# PXF JDBC
## 部署
1.将编译、打包后的pxf-jdbc.jar复制到/opt/pxf/lib/。
还有mysql的jdbc驱动包。

2.修改/etc/pxf/conf/pxf-private.classpath文件，增加一条路径记录：

    /opt/pxf/lib/pxf-jdbc-3.0.0.jar
    /opt/pxf/lib/mysql-connector-java-5.1.35-bin.jar

3.修改/etc/pxf/conf/pxf-profiles.xml，增加JDBC的profile（可选）：

    <profile>
        <name>JDBC</name>
        <description>A profile for writing data out of HAWQ via JDBC</description>
        <plugins>
            <fragmenter>com.insight.pxf.plugins.jdbc.JdbcFragmenterFactory</fragmenter>
            <accessor>com.insight.pxf.plugins.jdbc.JdbcAccessor</accessor>
            <resolver>com.insight.pxf.plugins.jdbc.JdbcResolver</resolver>
        </plugins>
    </profile>
使用profile后，创建PXF表时可以不用再指定accessor了。

## 重新启动pxf
在各节点重启。

    /opt/pxf/init.d/pxf-service restart
参考
## 使用
### 模板

    CREATE [READABLE|WRITABLE] EXTERNAL TABLE table_name
                ( column_name data_type [, ...] | LIKE other_table )
            LOCATION ('pxf://namenode[:port]/schema.tablename?Profile=JDBC'
                           '&JDBC_DRIVER=com.mysql.jdbc.Driver'
                           '&DB_URL=jdbc:mysql://192.168.200.6:3306/mysql&USER=root&PASS=root'
                           '&FRAGMENT_ROWS=1000'
                     )
参数说明：

    schema.tablename - 数据表名，在'pxf://'中设置
    JDBC_DRIVER      - jdbc驱动类名
    DB_URL           - jdbc连接的数据库url
    USER/PASS        - 数据库连接的用户名/密码
    FRAGMENT_ROWS    - 每个分片的查询数据量，可选，默认1000


### 部署mysql数据库

    docker run --name mysqldb --restart always -p 3306:3306\
           -e MYSQL_ROOT_PASSWORD=root \
           -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
           -v /docker-data/mysql:/var/lib/mysql \
           -d mysql:5.7.10 \
           --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci \
           --lower_case_table_names=1
    #启动之后创建测试数据表
    docker exec -ti mysqldb mysql -u root -p

    #创建数据库
    mysql> create database demodb;
    #创建表 -- 建立一个名为MyClass的表，
    字段名	数字类型	数据宽度	是否为空	是否主键	自动增加	默认值
    id	int	4	否	primary key	auto_increment
    name	char	20	否
    gender	int	4	否	 	 	0
    degree	double	16	是

    mysql> use demodb;
    mysql> create table myclass(
            id int(4) not null primary key,
            name varchar(20) not null,
            gender int(4) not null default '0',
            degree double(16,2));
    #插入测试数据
    insert into myclass values(1,"tom",1,90);
    insert into myclass values(2,'john',0,94);
    insert into myclass values(3,'simon',1,79);


### 创建pxf表连接mysql

    CREATE EXTERNAL TABLE myclass(id integer,
             name varchar,
             gender integer,
             degree float8)
             LOCATION ('pxf://localhost:51200/demodb.myclass'
                     '?PROFILE=JDBC'
                     '&JDBC_DRIVER=com.mysql.jdbc.Driver'
                     '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
                     )
             FORMAT 'CUSTOM' (Formatter='pxfwritable_import');

# PXF集群发现
## 说明
主要实现集群内PXF服务发现，收集当前运行PXF的节点信息。
 * 目的：JDBC表的数据分片
 * 起因：PXF本身是没有集群的概念，每个PXF都是独立管理的，PXF之间没有任何协调信息。

  HAWQ在对数据查询进行并行调度时是根据master->PXF返回的"分片元数据"来管理的。
  这个"分片元数据"信息包含了：数据所在的主机名及数据段落。然后HAWQ会调度某个segment去执行这个“分片元数据”。
  segment解析“分片元数据”，并连接“分片元数据”中的主机PXF去读取数据。也就是说数据所在的主机需要运行PXF（但可不运行segment）。

  而我们扩展的JDBC是用来连接已有的关系数据库，不可能在原有的关系数据库主机上运行PXF，因此我们需要一个机制发现当前的PXF实例，
 在返回的"分片元数据"中任意选择一个PXF实例地址做为主机名。也可以返回多个主机名、多个分片。


  实现方案：
 * 使用zookeeper服务发现。
 * 随机选择PXF实例做为分片主机，暂不考虑其他调度算法。
## 部署
### 启动zookeeper
使用容器启动，

    docker run -d --name=zkserver --hostname=zkserver --restart=always \
    -p 2181:2181 \
    -e MYID=1 \
    incloud/zookeeper:3.4.8

    集群启动可以加环境变量：`-e SERVERS=node-1,node-2,node-3`。
### jar包依赖
复制如下jar包到：pxf/lib

    curator-framework-2.9.1.jar
    curator-client-2.9.1.jar
    curator-recipes-2.9.1.jar
    zookeeper-3.4.6.jar


修改pxf-private.classpath，加入这些jar包：

    /opt/pxf/lib/curator-framework-2.9.1.jar
    /opt/pxf/lib/curator-client-2.9.1.jar
    /opt/pxf/lib/curator-recipes-2.9.1.jar
    /opt/pxf/lib/zookeeper-3.4.6.jar

### 修改pxf-service.war的web.xml
增加zookeeper相关设置:

    <context-param>
        <!--zookeeper服务器地址，集群模式可输入多个-->
        <param-name>zookeeper</param-name>
        <param-value>zkserver.docker:2181</param-value>
    </context-param>
    <listener>
        <listener-class>com.insight.pxf.plugins.PxfGroupListener</listener-class>
    </listener>

可以把这个配置放到pxf服务的配置文件->pxf-site.xml中。

### 重启pxf-service

# 数据库表分片问题
## 问题
在调用getFragments时，HAWQ MASTER并没有把Filter信息传送到PXF，因此统计出来的还是关系数据库表的全部行数。
但是PXF实际查询分片数据时会带有Filter，这样就会造成各分片PXF会查询到相同的数据。
## 解决
经过对hawq master源代码分析，定位到代码：/opt/incubator-hawq/src/backend/optimizer/plan/createplan.c文件，
方法：create_pxf_plan，其中的部分代码：

    segdb_work_map = map_hddata_2gp_segments(uri_str,
					  total_segs, segs_participating,
					  relation, NULL);
map_hddata_2gp_segments方法的定义：

    char** map_hddata_2gp_segments(char* uri, int total_segs, int working_segs, Relation relation, List* quals)

就是用来向PXF请求分片数量的，最后一个参数就是查询条件，我们修改create_pxf_plan的代码：

    segdb_work_map = map_hddata_2gp_segments(uri_str,
					  total_segs, segs_participating,
					  relation, ctx->root->parse->jointree->quals);
其中ctx->root就是查询计划，parse是查询树，typedef struct Query结构，jointree是`table join tree (FROM and WHERE clauses)`。

代码修改完毕，重新build。

# Oracle测试
## 启动数据库
以容器方式启动，下载容器镜像-https://hub.docker.com/r/sath89/oracle-12c/：

    docker pull sath89/oracle-12c
启动：

    docker run -d -p 8088:8080 -p 1521:1521 sath89/oracle-12c
持久化数据：

    docker run -d -p 8088:8080 -p 1521:1521 -v /data/oracle:/u01/app/oracle --restart always --name oracle sath89/oracle-12c

Connect database with following setting:

    hostname: localhost
    port: 1521
    sid: xe
    username: system
    password: oracle
Password for SYS & SYSTEM:

    oracle
Connect to Oracle Application Express web management console with following settings:

    http://localhost:8088/apex
    workspace: INTERNAL
    user: ADMIN
    password: 0Racle$

登录之后会提示修改密码，更改为：123456aA?

Connect to Oracle Enterprise Management console with following settings:

    http://localhost:8080/em
    user: sys
    password: oracle
    connect as sysdba: true

## 创建数据库表
使用web控制台，或其他工具创建表：

    CREATE TABLE sales (id int primary key, cdate date, amt decimal(10,2),grade varchar(30))
插入数据：

    INSERT INTO sales values (1, to_date('2008-01-01','yyyy-mm-dd'), 1000,'general');
    INSERT INTO sales values (2, to_date('2008-02-01','yyyy-mm-dd'), 900,'bad');
    INSERT INTO sales values (3, to_date('2008-03-10','yyyy-mm-dd'), 1200,'good');
    INSERT INTO sales values (4, to_date('2008-04-10','yyyy-mm-dd'), 1100,'good');
    INSERT INTO sales values (5, to_date('2008-05-01','yyyy-mm-dd'), 1010,'general');
    INSERT INTO sales values (6, to_date('2008-06-01','yyyy-mm-dd'), 850,'bad');
    INSERT INTO sales values (7, to_date('2008-07-01','yyyy-mm-dd'), 1400,'excellent');
    INSERT INTO sales values (8, to_date('2008-08-01','yyyy-mm-dd'), 1500,'excellent');
    INSERT INTO sales values (9, to_date('2008-09-01','yyyy-mm-dd'), 1000,'good');
    INSERT INTO sales values (10, to_date('2008-10-01','yyyy-mm-dd'), 800,'bad');
    INSERT INTO sales values (11, to_date('2008-11-01','yyyy-mm-dd'), 1250,'good');
    INSERT INTO sales values (12, to_date('2008-12-01','yyyy-mm-dd'), 1300,'excellent');
    INSERT INTO sales values (15, to_date('2009-01-01','yyyy-mm-dd'), 1500,'excellent');
    INSERT INTO sales values (16, to_date('2009-02-01','yyyy-mm-dd'), 1340,'excellent');
    INSERT INTO sales values (13, to_date('2009-03-01','yyyy-mm-dd'), 1250,'good');
    INSERT INTO sales values (14, to_date('2009-04-01','yyyy-mm-dd'), 1300,'excellent');
## hawq创建外部表

    gpadmin=#
    CREATE EXTERNAL TABLE sales(id integer,
             cdate date,
             amt float8,
             grade text)
             LOCATION ('pxf://localhost:51200/sales'
                     '?PROFILE=JDBC'
                     '&JDBC_DRIVER=oracle.jdbc.driver.OracleDriver'
                     '&DB_URL=jdbc:oracle:thin:@10.110.17.21:1521:xe&USER=sys as sysdba&PASS=oracle'
                     )
             FORMAT 'CUSTOM' (Formatter='pxfwritable_import');

复制oracle jdbc驱动到pxf应用lib目录，并修改路径文件/etc/pxf/conf/pxf-private.classpath，添加：

    /opt/pxf/lib/ojdbc6-12.1.0.2.jar
重启pxf service。

# 数据库分片
## 分页
默认的分片方式，根据查询条件计算数据库表的总行数，然后用分页技术划分若干页，每页就是一个Fragment。
### 实现类
实现类：`JdbcPageFragmenter`，Jdbc Profile的默认分片实现类，可不用设置。

### 参数
可设置分页大小、最大分片数，如下：

    LOCATION ('pxf://localhost:51200/mytable'
     '?PROFILE=JDBC'
     ....
     '&FRAGMENT_ROWS=10000'


## 数据库分区
关系数据库自身也会支持各类分区技术，因此我们也可以定义Fragmenter按照数据库分区关键字进行分片，从而可以提高并发读取性能。

目前支持日期型、整型、枚举型三类。
### 实现类
实现数据库分区的类是JdbcPartitionFragmenter，`需要在建外部表时设置-问题：如果设置了PROFILE，
pxf不允许在建表时更改为其他值`。

目前方案：自动判断如果带有`PARTITION_BY`参数则使用JdbcPartitionFragmenter，否则使用JdbcPageFragmenter。
### 参数
需要设置3个主要参数：

    PARTITION_BY参数：为关系数据库的列名和类型（可以不在hawq中定义），冒号分隔，类型：date、int、enum
    RANGE参数：设置起始日期，格式yyyy-MM-dd，冒号分隔。
    INTERVAL参数：分片间隔，可支持：day、month、year，前面可加整数值，冒号分隔
日期型示例：

    CREATE EXTERNAL TABLE psales(id integer,
             cdate date,
             amt float8)
             LOCATION ('pxf://localhost:51200/sales'
                     '?PROFILE=JDBC'
                     '&JDBC_DRIVER=oracle.jdbc.driver.OracleDriver'
                     '&DB_URL=jdbc:oracle:thin:@10.110.17.21:1521:xe&USER=sys as sysdba&PASS=oracle'
                     '&PARTITION_BY=cdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month'
                 )
             FORMAT 'CUSTOM' (Formatter='pxfwritable_import');
整型：

    ...
    &PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1
枚举型：

    ...
    &PARTITION_BY=level:enum&RANGE=excellent:good:general:bad
# SOLR外部表
参见[pxf-solr] READEME.

# pxf-site.xml配置参考


[pxf-solr]: pxf-solr/README_cn.md