/**
 * @Copyright to Hades.Yang 2015~2016.
 * @ClassName: CacheLayerClient.
 * @Project: AutoShardingDBPlatform.
 * @Package: generaldbplatform.
 * @Description: The basic client of the auto sharding cache layer.
 * @Author: Hades.Yang
 * @Version: V2.0
 * @Date: 2015-07-21
 * @History: 
 *    1.2015-07-21 First version of CacheLayerClient was written.
 *    2.2015-08-13 Modify subscribe interface,add ConfigDBClient support. 
 */
//package name.
package generaldbplatform;

//class import for java utilities.
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//class import for jedis client.
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisSentinelPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * @ClassName: CacheLayerClient.
 * @Description: this class is used to define the auto-extended & auto-sharded cache layer.
 *               (current cache database is Redis, but it can be changed to other cache database).
 */
public class CacheLayerClient 
{
    /**
     * @FieldName: SentinelPool_TEMP.
     * @Description: the temp redis sentinel pool which is used when connect redis server when initialize the CacheShardingMap.
     */
    private JedisSentinelPool SentinelPool_TEMP=null;
    
    /**
     * @FieldName: PoolConfig.
     * @Description: Member variable,the config object which is used when connect redis server.
     */ 
    private GenericObjectPoolConfig PoolConfig;
    
    /**
     * @FieldName: CurrentCacheMapIndex.
     * @Description: the index of the CacheShardingMap,which to be inserted.
     *               (when new node information comes,this index shows HashMap index with the JedisSentinelPool)
     */
    private int CurrentCacheMapIndex = 0;  //the index of map tobe insert.
    
    /**
     * @FieldName:SentinelPoolTimeout.
     * @Description: Member variable,the timeout value which is used when create the JedisSentinelPool.
     */
    private int SentinelPoolTimeout = 100000;
	
    /**
     * @FieldName: configdb.
     * @Description: the ConfigServer database client.
     */
    private ConfigDBClient configdb;
	
    /**
     * @FieldName: CACHE_SERVER_NUM_OLD.
     * @Description: the new mod_number which is used to store/fetch value from the old HashMap.
     */
    protected int CACHE_SERVER_NUM_OLD = 1;
	
    /**
     * @FieldName: CACHE_SERVER_NUM_NEW.
     * @Description: the new mod_number which is used to store/fetch value from the new HashMap.
     */
     protected int CACHE_SERVER_NUM_NEW = 1;
    
    /**
     * @FieldName: CacheShardingMap.
     * @Description: Member variable,the map which is used for sharding function.
     */
    protected Map<Integer, JedisSentinelPool> CacheShardingMap;  
    
    /**
     * @FieldName: CachaInitOK.
     * @Description: the boolean value which shows the cache database client initialize ok or not.
     */
    protected boolean CacheInitOK = false;
	
    /**
     * @Title: ConfigDBInit.
     * @Description: this function is used to initialize the configdb.
     * @param serverinfo_0: the first redis server node ip & port information in the redis sentinel.
     * @param serverinfo_1: the second redis server node ip & port information in the redis sentinel.
     * @param serverinfo_2: the third redis server node ip & port information in the redis sentinel.
     * @return none.
     */
    private void ConfigDBInit(String serverinfo_0, String serverinfo_1, String serverinfo_2)
    {
	this.configdb = new ConfigDBClient(serverinfo_0, serverinfo_1, serverinfo_2);
    }

    /**
     * @Title: Subscriber.
     * @Description: the function which is used to subscribe one channel from redis server.
     * @param ip: the redis server ip address. 
     * @param channel: the specific channel which was subscribed by the application.
     * @return none.
     */
    private void Subscriber(final String channel)
    {
	final JedisPubSub jedisPubSub = new JedisPubSub() 
	{
	    @Override	
	    public void onMessage(String channel, String message) 
	    {  
	        //System.out.println("Input channel =="+channel + "----input message ==" + message); 
		if(channel.equals("CACHE_AUTO_EXTERN"))
		{
		    CacheShardMapInit(message);
		}
            }  
	};
	new Thread(new Runnable() 
	{
	    @Override
	    public void run() 
	    {
		//System.out.println("Start!!!"); 
                Jedis jedisConnector = null;
		boolean borrowOrOprSuccess = true;
		
		try 
		{
		    jedisConnector = configdb.db_client.getResource();
		    jedisConnector.subscribe(jedisPubSub, channel);
		}
		catch(JedisConnectionException e)
		{
		    borrowOrOprSuccess = false;
	            if(jedisConnector != null)
		    {
			configdb.db_client.returnBrokenResource(jedisConnector);
			jedisConnector = null;
		    }
		    throw e;
		}
		finally
		{
	            if(borrowOrOprSuccess && (jedisConnector!=null))
	            {
			configdb.db_client.returnResource(jedisConnector);
		    }
		}
	    }
	}, "subscriberThread").start();
    }
    
    /**
     * @Title: CacheShardMapInit.
     * @Description: the function is used to initialize the CacheShardingMap.(when the client got the info_message)
     * @param info_message:the message which was published from the redis server and contain the cache server info such as ip & port.
     *        the message should be like this:"1.0.0.1:6379_1.0.2.3:6349_1.0.0.5:5689;1.0.0.22:6379_1.10.2.3:6349_12.0.0.5:5689".
     * @return none.
     */
    public void CacheShardMapInit(String info_message)
    {
	//create a temp set of String to store redis HostAndPort info.
	Set<String> sentinel_temp = new HashSet<String>();
		
	//split the info_message like this:"1.0.0.1:6379_1.0.2.3:6349_1.0.0.5:5689",3 ip:port segments.
	String[] arr_new_node = info_message.split(";");  //must split by ";".
		
	//Assign the old mod number to CACHE_SERVER_NUM_OLD.
	this.CACHE_SERVER_NUM_OLD = CacheShardingMap.size();
		
	for(int i = 0; i < arr_new_node.length; i++)
	{
	    //clear the old HostAndPort info.
	    sentinel_temp.clear();
			
	    //split the "1.0.0.1:6379_1.0.2.3:6349_1.0.0.5:5689" like:"1.0.0.1:6379".
	    String[] arr_ip_port = arr_new_node[i].split("_");
			
	    //split the "1.0.0.1:6379" like: ip->1.0.0.1, port->6379.
	    String ip0 = arr_ip_port[0].split(":")[0];
	    int port0 = Integer.parseInt(arr_ip_port[0].split(":")[1]);
	    String ip1 = arr_ip_port[1].split(":")[0];
	    int port1 = Integer.parseInt(arr_ip_port[1].split(":")[1]);
	    String ip2 = arr_ip_port[2].split(":")[0];
	    int port2 = Integer.parseInt(arr_ip_port[2].split(":")[1]);
			
	    //Initialize the sentinel host and port info.
	    sentinel_temp.add(new HostAndPort(ip0,port0).toString());
	    sentinel_temp.add(new HostAndPort(ip1,port1).toString());
	    sentinel_temp.add(new HostAndPort(ip2,port2).toString());
			
	    //Create a new JedisSentinelPool instance and insert it into CacheShardingMap with the latest mod index.
	    SentinelPool_TEMP = new JedisSentinelPool("mymaster"+CurrentCacheMapIndex, sentinel_temp, this.PoolConfig, this.SentinelPoolTimeout);
	    CacheShardingMap.put(CurrentCacheMapIndex, SentinelPool_TEMP);
	    CurrentCacheMapIndex += 1;
	}
		
        //check the CacheShardingMap size and assign the value to CACHE_SERVER_NUM_NEW.
	if(CacheShardingMap.size()>0)
	{
	    this.CACHE_SERVER_NUM_NEW = CacheShardingMap.size(); 
	    this.CacheInitOK = true;
	}
    }
	
	
    /**
     * @Title: getOldShardedCacheClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_OLD.
     * @param key: int value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
    public JedisSentinelPool getOldShardedCacheClient(int key)
    {
        return this.CacheShardingMap.get(key % this.CACHE_SERVER_NUM_OLD);
    }
	
    /**
     * @Title: getNewShardedCacheClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_NEW.
     * @param key: int value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
    public JedisSentinelPool getNewShardedCacheClient(int key)
    {
	return this.CacheShardingMap.get(key % this.CACHE_SERVER_NUM_NEW);
    }
	
    /**
     * @Title: getOldShardedCacheClient
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_OLD.
     * @param key:long value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
    public JedisSentinelPool getOldShardedCacheClient(long key)
    {
        return this.CacheShardingMap.get(((int)(key & 0xFF)) % this.CACHE_SERVER_NUM_OLD);
    }
	
    /**
     * @Title: getNewShardedCacheClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_NEW.
     * @param key:long value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
	public JedisSentinelPool getNewShardedCacheClient(long key)
	{
		return this.CacheShardingMap.get(((int)(key & 0xFF)) % this.CACHE_SERVER_NUM_NEW);
	}
	
    /**
     * @Title: getOldShardedCacheClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_OLD.
     * @param key: String value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
	public JedisSentinelPool getOldShardedCacheClient(String key)
	{
		return this.CacheShardingMap.get((int)(key.toCharArray()[key.length()-1]) % this.CACHE_SERVER_NUM_OLD);
	}
	
    /**
     * @Title: getNewShardedCacheClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the CACHE_SERVER_NUM_NEW.
     * @param key: String value which is indicates the value.
     * @return JedisSentinelPool: the Redis sentinel client which is used to access the Redis sentinel.
     */
	public JedisSentinelPool getNewShardedCacheClient(String key)
	{
		return this.CacheShardingMap.get((int)(key.toCharArray()[key.length()-1]) % this.CACHE_SERVER_NUM_NEW);
	}
	
    /**
     * @Title: CacheLayerClient.
     * @Description: the construct function of the CacheLayerClient class.
     * @param serverinfo_0: the first redis server node ip & port information in the redis sentinel.
     * @param serverinfo_1: the second redis server node ip & port information in the redis sentinel.
     * @param serverinfo_2: the third redis server node ip & port information in the redis sentinel.
     * @param channel: the channel that subscribed by the CacheLayerClient.
     * @return none.
     */
    public CacheLayerClient(String serverinfo_0, String serverinfo_1, String serverinfo_2, String channel)
    {
		this.PoolConfig = new GenericObjectPoolConfig();
		this.PoolConfig.setMaxIdle(25);
		this.PoolConfig.setMaxTotal(250);
		this.PoolConfig.setMaxWaitMillis(10000);
		this.ConfigDBInit(serverinfo_0, serverinfo_1, serverinfo_2);
    	this.CacheShardingMap = new HashMap<Integer, JedisSentinelPool>();
    	this.Subscriber(channel);
    }
}
