# README

implements hawq pxf jdbc read plugin  。
more detail -> [chinese readme].
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

 [chinese readme]: /README_cn.md