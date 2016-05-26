package com.insight.pxf.cluster;

/**
 * Created by jiadx on 2016/5/17.
 * 主要实现集群内PXF服务发现，收集当前运行PXF的节点信息。
 * 目的：JDBC表的数据分片
 * 起因：PXF本身是没有集群的概念，每个PXF都是独立管理的，PXF之间没有任何协调信息。
 * HAWQ在对数据查询进行并行调度时是根据master->PXF返回的"分片元数据"来管理的。
 * 这个"分片元数据"信息包含了：数据所在的主机名及数据段落。然后HAWQ会调度某个segment去执行这个“分片元数据”。
 * <p/>
 * segment解析“分片元数据”，并连接“分片元数据”中的主机PXF去读取数据。也就是说数据所在的主机需要运行PXF（但可不运行segment）。
 * <p/>
 * 而我们扩展的JDBC是用来连接已有的关系数据库，不可能在原有的关系数据库主机上运行PXF，因此我们需要一个机制发现当前的PXF实例，
 * 在返回的"分片元数据"中任意选择一个PXF实例地址做为主机名。也可以返回多个主机名、多个分片。
 * <p/>
 * 暂不考虑其他调度算法。
 * <p/>
 * 实现方案：
 * 使用zookeeper服务发现。
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.net.*;
import java.util.Set;

public class PxfGroupListener implements ServletContextListener {
    private static final Log LOG = LogFactory.getLog(PxfGroupListener.class);
    static PxfGroupListener _instance = null;
    static String ZK_ROOT = "/pxf-server-hosts";

    GroupMember pxfGroup = null;
    ServletContext context = null;
    CuratorFramework zk_client = null;

    //当前服务在Group中的标识，格式：主机IP地址+端口
    String thisId = null;
    byte[] payload = new byte[0]; //只使用路径

    public static PxfGroupListener instance() {
        return _instance;
    }
    public static String[] getPxfMembers(){
        if(_instance != null )
            return _instance.getAllServices();
        else
            return new String[]{"localhost"};
    }

    public void contextInitialized(ServletContextEvent arg0) {
        _instance = this;
        context = arg0.getServletContext();
        this.thisId = getLocalAddr(context);

        String zk_hosts = context.getInitParameter("zookeeper");
        zk_client = CuratorFrameworkFactory.newClient(zk_hosts, new ExponentialBackoffRetry(5000, Integer.MAX_VALUE));
        zk_client.start();

        pxfGroup = new GroupMember(zk_client, ZK_ROOT, thisId, payload);
        pxfGroup.start();

        //处理zookeeper服务失效的状况(如果设置了一直重试的策略后，不会再调用listener)
        zk_client.getConnectionStateListenable().addListener(new ConnectionStateListener(){
            @Override
            public void stateChanged(CuratorFramework framework, ConnectionState state) {
                switch (state){
                    case CONNECTED:
                    case RECONNECTED:
                        if(LOG.isDebugEnabled())
                            LOG.debug("zookeeper server is RECONNECTED");
                        //pxfGroup = new GroupMember(framework, ZK_ROOT, thisId, payload);
                        pxfGroup.start();
                        break;
                    case LOST:
                        pxfGroup.close(); //会有内存泄漏吗？
                        if(LOG.isDebugEnabled())
                            LOG.debug("zookeeper server is LOST");
                        break;
                    case SUSPENDED:
                        break;
                    case READ_ONLY:
                        break;
                }
            }
        });

        System.out.println(String.format("ServletContext[%s] is inited...", context.getServerInfo()));
    }

    public void contextDestroyed(ServletContextEvent arg0) {
        ServletContext context = arg0.getServletContext();
        if (zk_client != null) {
            pxfGroup.close();
            zk_client.close();
        }
        System.out.println(String.format("ServletContext[%s] is destoryed...", context.getServerInfo()));

    }

    //返回所有PXF服务实例的主机名
    public String[] getAllServices() {
        try {
            Set<String> members = pxfGroup.getCurrentMembers().keySet();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Pxf Group Members: " + members);
            }
            return members.toArray(new String[0]);
        } catch (Exception e) {
            LOG.error("get pxf instance error:", e);
            return new String[]{getLocalAddr(context)};
        }
    }

    String getLocalAddr(ServletContext context) {
        try {
            InetAddress netAddress = InetAddress.getLocalHost();
            String ip = netAddress.getHostAddress();
            String name = netAddress.getHostName();
            return ip;
        } catch (UnknownHostException e) {
            LOG.error("get hostname  error:", e);
        }
        /*
        try {
            Enumeration addresses = NetworkInterface.getByName("eth0").getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address) {
                    //System.out.println("本机的IP = " + ip.getHostAddress());
                    return ip.getHostAddress();
                }
            }

        } catch (SocketException e) {
            LOG.error("get network address error:",e);
            //e.printStackTrace();
        }
        */
        return "localhost";
    }
}
