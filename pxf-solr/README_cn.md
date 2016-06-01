# 说明


# SOLR外部表
## 配置
修改pxf-profiles.xml，增加SOLR的profile：

    <profile>
        <name>SOLR</name>
        <plugins>
            <fragmenter>com.insight.pxf.plugins.solr.SolrFragmenter</fragmenter>
            <accessor>com.insight.pxf.plugins.solr.SolrReadAccessor</accessor>
            <resolver>com.insight.pxf.plugins.solr.SolrReadResolver</resolver>
        </plugins>
    </profile>
修改pxf-private.classpath，加入这些jar包：

    /opt/pxf/lib/pxf-solr-3.0.0.jar
    /opt/pxf/lib/solr-solrj-5.3.1.jar
    /opt/pxf/lib/httpclient-4.4.1.jar
    /opt/pxf/lib/httpcore-4.4.1.jar
    /opt/pxf/lib/httpmime-4.4.1.jar
    /opt/pxf/lib/stax2-api-3.1.4.jar
    /opt/pxf/lib/woodstox-core-asl-4.4.1.jar
    /opt/pxf/lib/commons-httpclient-3.1.jar
    /opt/pxf/lib/commons-net-3.1.jar
    /opt/pxf/lib/noggit-0.6.jar

## 格式
使用`PROFILE=SOLR`，主要参数：

    URL - solr引擎地址，必选
    MAX_ROWS - 符合条件的最大行数，默认1000，可选
    FRAGMENT_ROWS - 每个页（分片）的行数，默认100，可选

    ##MIN_SCORE - 文档的最低评分，默认0.5，可选 --不可用，score的评分值只是多个文档之间互相比较时有意义

注意URL地址不要以`/`结尾，因为完整的SOLR地址=`URL`+`/`+`输入源`。

## 示例

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



# solr部署参考
## 使用docker部署solr

    docker pull solr:5.3.2

单节点（以下操作为单机模式）:

    docker run --name solr -d -p 8985:8983 --restart always \
          -v /opt/solrconf:/opt/solrconf \
          solr:5.3.2 \
          sleep infinity

    #进入容器再启动solr（因为我们需要修改solr配置，并重新启动solr服务）：
    docker exec -d solr /opt/solr/bin/solr start

    #创建collection：
    docker exec -i -t solr /opt/solr/bin/solr create_core -c sales
web管理UI：

    http://localhost:8985/solr/#/

## 导入数据-mysql
数据模型-销售数据

    域名	   是否分词	是否存储	是否索引	说明
    id	    no	    yes	    yes	    主键,int型
    cdate	no	    yes	    yes	    日期,date型
    amt 	no	    yes	    no      销售量,float型
    grade	no	    yes	    yes	    评级，string型
    note	yes	    yes	    yes	    注释，string
solr schema:

	....
	<fieldtype name="string" class="solr.StrField" sortMissingLast="true" omitNorms="true"/>
	<fieldtype name="sint" class="solr.SortableIntField" sortMissingLast="true" omitNorms="true"/>
	<fieldtype name="sfloat" class="solr.SortableFloatField" sortMissingLast="true" omitNorms="true"/>
	<fieldtype name="date" class="solr.DateField" sortMissingLast="true" omitNorms="true"/>
	<fieldtype name="text_ws" class="solr.TextField">
			<analyzer type="index" class="org.apache.lucene.analysis.cjk.CJKAnalyzer">
				<!--<tokenizer class="org.apache.lucene.analysis.cjk.CJKTokenizer"/>-->
			</analyzer>
			<analyzer type="query" class="org.apache.lucene.analysis.cjk.CJKAnalyzer">
			</analyzer>
	</fieldtype>


	...
	<fields>
       <field name="id" type="string" indexed="true" stored="true" multiValued="false" required="true"/>
       <field name="cdate" type="string" indexed="true" stored="true" multiValued="false" />
       <field name="amt" type="string" indexed="false" stored="true" multiValued="false" />
       <field name="grade" type="string" indexed="true" stored="true" multiValued="false" />
       <field name="note" type="text_general" indexed="true" stored="true" omitNorms="false"/>
    </fields>

    <uniqueKey>id</uniqueKey>
    <defaultSearchField>cdate</defaultSearchField>

复制数据导入的jar文件到/opt/solr/server/lib ，（mysql需要自己下载）：

    //cp /opt/solr/dist/solr-dataimporthandler-5.3.2.jar /opt/solr/server/solr/lib
    //cp /opt/solrconf/mysql-connector-java-5.1.35-bin.jar /opt/solr/server/solr/lib

    #需要复制到webapp的lib目录:
    cp /opt/solr/dist/solr-dataimporthandler-5.3.2.jar /opt/solr/server/solr-webapp/webapp/WEB-INF/lib
    cp /opt/solrconf/mysql-connector-java-5.1.35-bin.jar /opt/solr/server/solr-webapp/webapp/WEB-INF/lib

    #因为使用了int型，启动时报错：SolrException: Invalid Number: MA147LL/A：
    需修改elevate.xml中的字符串部分为整数。

重启solr server（容器内执行）:

    /opt/solr/bin/solr restart

从UI执行数据导入：

    http://10.110.17.21:8985/solr/#/sales/dataimport//dataimport