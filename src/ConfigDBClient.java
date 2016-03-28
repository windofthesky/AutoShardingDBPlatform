/**
 * @Copyright to Hades.Yang 2015~2016.
 * @ClassName: ConfigDBClient.
 * @Project: AutoShardingDBPlatform.
 * @Package: generaldbplatform.
 * @Description: The database client for AutoShardingDBPlatform.
 * @Author: Hades.Yang 
 * @Version: V1.0
 * @Date: 2015-08-12
 * @History: 
 *    1.2015-08-12 First version of ConfigDBClient was written.
 */
 
//package name.
package generaldbplatform;

//import for java utilities.
import java.util.HashSet;
import java.util.Set;

//import for jedis components.
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisSentinelPool;

//import for common pool.
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


/**
 * @ClassName: ConfigDBClient.
 * @Description: this class is used to connect redis sentinel and offer the way to access database for ConfigClient & ConfigMonitor.
 */
public class ConfigDBClient 
{
    /**
     * @FieldName: ip_0.
     * @Description: the ip address of the first redis server on the redis sentinel.
     */
    private String ip_0;

    /**
     * @FieldName: port_0.
     * @Description: the service port of the first redis server on the redis sentinel.
     */	
    private int port_0;

    /**
     * @FieldName: ip_1.
     * @Description: the ip address of the second redis server on the redis sentinel.
     */	
    private String ip_1;
	
    /**
     * @FieldName: port_1.
     * @Description: the service port of the second redis server on the redis sentinel.
     */
    private int port_1;
	
    /**
     * @FieldName: ip_2.
     * @Description: the ip address of the third redis server on the redis sentinel.
     */
    private String ip_2;
	
    /**
     * @FieldName: port_2.
     * @Description: the service port of the third redis server on the redis sentinel.
     */
    private int port_2;
	
    /**
     * @FieldName: PoolConfig. 
     * @Description: the general pool config of redis  sentinel. 
     */
    private GenericObjectPoolConfig PoolConfig;
	
    /**
     * @FieldName: SentinelPoolTimeout.
     * @Description: the default timeout which is used to initialize the JedisSentinelPool. 
     */
    private int SentinelPoolTimeout = 100000;
	
    /**
     * @FieldName: db_client.
     * @Description: the jedis sentinel client which is used to connect the redis sentinel.
     */
    protected JedisSentinelPool db_client;
	
    /**
     * @Title: ConfigDBClient.
     * @Description: the construct function which is used to initialize the object.
     * @param serverinfo_0: the first redis server node ip & port information in the redis sentinel.
     * @param serverinfo_1: the second redis server node ip & port information in the redis sentinel.
     * @param serverinfo_2: the third redis server node ip & port information in the redis sentinel.
     * @return none.
     */
    public ConfigDBClient(String serverinfo_0, String serverinfo_1, String serverinfo_2)
    {
	Set<String> HostAndPort_Set = new HashSet<String>();
	
	this.ip_0 = serverinfo_0.split(":")[0];
	this.port_0 = Integer.parseInt(serverinfo_0.split(":")[1]);
	
	this.ip_1 = serverinfo_1.split(":")[0];
	this.port_1 = Integer.parseInt(serverinfo_1.split(":")[1]);
	
	this.ip_2 = serverinfo_2.split(":")[0];
	this.port_2 = Integer.parseInt(serverinfo_2.split(":")[1]);
	
	HostAndPort_Set.add(new HostAndPort(ip_0,port_0).toString());
	HostAndPort_Set.add(new HostAndPort(ip_1,port_1).toString());
	HostAndPort_Set.add(new HostAndPort(ip_2,port_2).toString());
		
	this.PoolConfig = new GenericObjectPoolConfig();
	this.PoolConfig.setMaxIdle(25);
	this.PoolConfig.setMaxTotal(250);
	this.PoolConfig.setMaxWaitMillis(10000);
		
	this.db_client = new JedisSentinelPool("mymaster", HostAndPort_Set, this.PoolConfig, this.SentinelPoolTimeout);
    }
}
