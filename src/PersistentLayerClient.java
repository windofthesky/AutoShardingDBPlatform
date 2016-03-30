/**
 * @Copyright to Hades.Yang 2015~2016.
 * @ClassName: PersistentLayerClient.
 * @Project: AutoShardingDBPlatform.
 * @Package: generaldbplatform.
 * @Description: The basic client of the auto sharding persistent layer.
 * @Author: Hades.Yang 
 * @Version: V1.0
 * @Date: 2015-07-21
 * @History: 
 *    1.2015-07-21 First version of PersistentLayerClient was written.
 *    2.2015-08-13 Modify subscribe interface,add ConfigDBClient support. 
 */

package generaldbplatform;

//class import for java utilities.
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//class import for mongodb client.
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.ServerAddress;

//class import for jedis client.
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class PersistentLayerClient 
{
    /**
     * @FieldName: mongoOption.
     * @Description: the config option for each of the MongoClient,shows the basic connection information.
     */
    private MongoClientOptions mongoOption = null;
    
    /**
     * @FieldName: CurrentPersistMapIndex.
     * @Description: the index of the PersistShardingMap,which to be inserted.
     *               (when new node information comes,this index shows HashMap index with the MongoClient)
     */
    private int CurrentPersistMapIndex = 0;  
    
    /**
     * @FieldName: mongoClient_temp.
     * @Description: the temp MongoClient, which is used as a temp variable.
     */
    private MongoClient mongoClient_temp = null;
	
    /**
     * @FieldName: configdb.
     * @Description: the ConfigServer database client.
     */
    private ConfigDBClient configdb;	
    
    /**
     * @FieldName: PERSIST_SERVER_NUM_OLD.
     * @Description: the old mod-number which is used to store/fetch value from the old HashMap.
     */
    protected int PERSIST_SERVER_NUM_OLD = 1;
	
    /**
     * @FieldName: PERSIST_SERVER_NUM_NEW.
     * @Description: the new mod_number which is used to store/fetch value from the new HashMap.
     */
    protected int PERSIST_SERVER_NUM_NEW = 1;
	
    /**
     * @FieldName: PersistShardingMap.
     * @Description: the HashMap which is used to store the different MongoClient.
     *               the MongoClient connected with a MongoDB replica-set.
     */
    protected Map<Integer, MongoClient> PersistShardingMap;  
    
    /**
     * @FieldName: PersistInitOK.
     * @Description: the boolean value which shows the persistent database client initialize ok or not.
     */
    protected boolean PersistInitOK = false;
	
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
		        
		if(channel.equals("PERSIST_AUTO_EXTERN"))
		{
		    PersistentShardMapInit(message);
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
     * @Title: PersistentShardMapInit.
     * @Description: the function is used to initialize the PersistentShardingMap.(when the client got the info_message)
     * @param info_message:the message which was published from the redis server and contain the persistent server info such as ip & port.
     *        the message should be like this:"1.0.0.1:27018_1.0.2.3:27017_1.0.0.5:27019;1.0.0.22:27017_1.10.2.3:27018_12.0.0.5:27019".
     * @return none.
     */
    public void PersistentShardMapInit(String info_message)
    {
        //the list consists of ServerAddress which records the MongoDB ip & port information.
	List<ServerAddress> server_addresses = new ArrayList<ServerAddress>();
	
	//split the info_message like this:"1.0.0.1:27017_1.0.2.3:27018_1.0.0.5:27019",3 ip:port segments.
	String[] arr_new_node = info_message.split(";");  //must split by ";".
			
	//Assign the old mod number to CACHE_SERVER_NUM_OLD.
	this.PERSIST_SERVER_NUM_OLD = PersistShardingMap.size();
				
	for(int i = 0; i < arr_new_node.length; i++)
	{
	    //clear the old address info.
	    server_addresses.clear();
			
	    //split the "1.0.0.1:27017_1.0.2.3:27018_1.0.0.5:27019" like:"1.0.0.1:27017".
	    String[] arr_ip_port = arr_new_node[i].split("_");
					
	    //split the "1.0.0.1:27017" like: ip->1.0.0.1, port->27017.
	    String ip0 = arr_ip_port[0].split(":")[0];
	    int port0 = Integer.parseInt(arr_ip_port[0].split(":")[1]);
	    String ip1 = arr_ip_port[1].split(":")[0];
	    int port1 = Integer.parseInt(arr_ip_port[1].split(":")[1]);
	    String ip2 = arr_ip_port[2].split(":")[0];
	    int port2 = Integer.parseInt(arr_ip_port[2].split(":")[1]);
			
	    //construct the 3 MongoDB address instances.
	    ServerAddress address0 = new ServerAddress(ip0, port0);
	    ServerAddress address1 = new ServerAddress(ip1, port1);
	    ServerAddress address2 = new ServerAddress(ip2, port2);
	    server_addresses.add(address0);
	    server_addresses.add(address1);
	    server_addresses.add(address2);
			
	    //create a new MongoClient instance and insert it into PersistShardingMap with the latest mod index.
	    mongoClient_temp = new MongoClient(server_addresses,this.mongoOption);
	    this.PersistShardingMap.put(CurrentPersistMapIndex, mongoClient_temp);
	    CurrentPersistMapIndex += 1;
	}
			
	//check the sharding map size and assign the value to PERSIST_SERVER_NUM_NEW.
	if(PersistShardingMap.size()>0)
        {
	    this.PERSIST_SERVER_NUM_NEW = PersistShardingMap.size(); 
	    this.PersistInitOK = true;
	}
    }

    /**
     * @Title: getOldShardedPersistClient.
     * @Description: this function is used to fetch the MongoClient mod by the PERSIST_SERVER_NUM_OLD.
     * @param key: int value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getOldShardedPersistClient(int key)
    {
	return this.PersistShardingMap.get(key % this.PERSIST_SERVER_NUM_OLD);
    }
	
    /**
     * @Title: getNewShardedPersistClient.
     * @Description: this function is used to fetch the MongoClient mod by the PERSIST_SERVER_NUM_NEW.
     * @param key: int value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getNewShardedPersistClient(int key)
    {
	return this.PersistShardingMap.get(key % this.PERSIST_SERVER_NUM_NEW);
    }
	
    /**
     * @Title: getOldShardedPersistClient.
     * @Description: this function is used to fetch the MongoClient mod by the PERSIST_SERVER_NUM_OLD.
     * @param key: long value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getOldShardedPersistClient(long key)
    {
	return this.PersistShardingMap.get(((int)(key & 0xFF)) % this.PERSIST_SERVER_NUM_OLD);
    }
	
    /**
     * @Title: getNewShardedPersistClient.
     * @Description: this function is used to fetch the MongoClient mod by the PERSIST_SERVER_NUM_NEW.
     * @param key: long value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getNewShardedPersistClient(long key)
    {
	return this.PersistShardingMap.get(((int)(key & 0xFF)) % this.PERSIST_SERVER_NUM_NEW);
    }
	
    /**
     * @Title: getOldShardedPersistClient.
     * @Description: the function which is used to fetch the JedisSentinelPool mod by the PERSIST_SERVER_NUM_OLD.
     * @param key: String value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getOldShardedPersistClient(String key)
    {
	return this.PersistShardingMap.get((int)(key.toCharArray()[key.length()-1]) % this.PERSIST_SERVER_NUM_OLD);
    }
	
    /**
     * @Title: getNewShardedPersistClient.
     * @Description: the function which is used to fetch the MongoClient mod by the PERSIST_SERVER_NUM_NEW.
     * @param key: String value which is indicates the value.
     * @return MongoClient:the MongoDB client which is used to access the MongoDB.
     */
    public MongoClient getNewShardedPersistClient(String key)
    {
	return this.PersistShardingMap.get((int)(key.toCharArray()[key.length()-1]) % this.PERSIST_SERVER_NUM_NEW);
    }
	
    /**
     * @Title: PersistentLayerClient.
     * @Description: the construct function of this PersistentLayerClient class.
     * @param serverinfo_0: the first redis server node ip & port information in the redis sentinel.
     * @param serverinfo_1: the second redis server node ip & port information in the redis sentinel.
     * @param serverinfo_2: the third redis server node ip & port information in the redis sentinel.
     * @param channel: the channel that subscribed by the PersistentLayerClient.
     * @return none.
     */
    public PersistentLayerClient(String serverinfo_0, String serverinfo_1, String serverinfo_2, String channel)
    {
	this.mongoOption = new MongoClientOptions.Builder().socketKeepAlive(true)
				           .connectTimeout(50000)
				           .socketTimeout(30000)
				           .readPreference(ReadPreference.primary())
				           .connectionsPerHost(80)
				           .maxWaitTime(1000*60*2)
				           .threadsAllowedToBlockForConnectionMultiplier(40)
				           .writeConcern(WriteConcern.NORMAL).build();
	this.ConfigDBInit(serverinfo_0, serverinfo_1, serverinfo_2);
	this.PersistShardingMap = new HashMap<Integer, MongoClient>();
	this.Subscriber(channel);
    }
}
