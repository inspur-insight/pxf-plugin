# README

implements hawq pxf jdbc read plugin  AND pxf solr plugin。

more detail -> [chinese readme].

# build

    gradlew jar

# Config
## modify File - /etc/pxf/conf/pxf-profiles.xml

    <profile>
            <name>JDBC</name>
            <description>A profile for writing data out of HAWQ via JDBC</description>
            <plugins>
                <fragmenter>com.insight.pxf.plugins.jdbc.JdbcFragmenterFactory</fragmenter>
                <accessor>com.insight.pxf.plugins.jdbc.JdbcAccessor</accessor>
                <resolver>com.insight.pxf.plugins.jdbc.JdbcResolver</resolver>
            </plugins>
        </profile>
## modify file - /etc/pxf/conf/pxf-private.classpath
    /opt/pxf/lib/pxf-jdbc-3.0.0.jar
    /opt/pxf/lib/mysql-connector-java-5.1.35-bin.jar
## restart pxf
    service pxf-service restart
# example1

MySql Table:

    create table myclass(
                id int(4) not null primary key,
                name varchar(20) not null,
                gender int(4) not null default '0',
                degree double(16,2));
Hawq external Table:

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
# example2
Oracle Table:

    CREATE TABLE sales (id int primary key, cdate date, amt decimal(10,2),grade varchar(30))
HAWQ external Table:

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

# example3-solr

    CREATE EXTERNAL TABLE ssales(id integer,
             cdate date,
             amt float8,
             grade text,
             score real)
             LOCATION ('pxf://localhost:51200/sales'
                     '?PROFILE=SOLR'
                     '&URL=http://10.110.17.21:8985/solr'
                     )
             FORMAT 'CUSTOM' (Formatter='pxfwritable_import');

 [chinese readme]: /README_cn.md