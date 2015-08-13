/**
 * @Copyright to Hades.Yang 2015~2020.
 * @ClassName: GeneralDBClient.
 * @Project: AutoShardingDBPlatform.
 * @Package: generaldbplatform.
 * @Description: The basic client of the auto sharding general database platform.
 * @Author: Hades.Yang 
 * @Version: V1.0
 * @Date: 2015-07-22
 * @History: 
 *    1.2015-07-21 First version of GeneralDBClient was written.
 *    2.2015-08-13 Modify the constructor function add ConfigDBClient support.
 */

//package name.
package generaldbplatform;


public class GeneralDBClient 
{
	/**
     * @FieldName: mCacheClient.
     * @Description: the cache database client as a private member, just can be used by the GeneralDBClient itself.
     *               (when we use the client, we get the cache database real client according to the key).
     */
    private CacheLayerClient mCacheClient;
    
	/**
     * @FieldName: mPersistClient.
     * @Description: the persistent database client as a private member,just can be used by the GeneralDBClient itself.
     *               (when we use the client, we get the persistent database real client according to the key).
     */
    private PersistentLayerClient mPersistClient;
    
	/**
     * @FieldName: allClientOK.
     * @Description: a boolean value which indicates the states of the CacheClient & PersistClient.
     *               (when both ok, and this boolean value should be true).
     *               the GeneralDBClient user could check this value to decide when to start the service.
     */
    protected boolean allClientOK = false;
    
    

    
    /**
     * @Title: PersistentLayerClient.
     * @Description: the construct function of this PersistentLayerClient class.
	 * @param serverinfo_0: the first redis server node ip & port information in the redis sentinel.
	 * @param serverinfo_1: the second redis server node ip & port information in the redis sentinel.
     * @param serverinfo_2: the third redis server node ip & port information in the redis sentinel.
	 * @param cache_channel: the channel that subscribed by the CacheLayerClient.    
	 * @param persist_channel: the channel that subscribed by the PersistentLayerClient.
     * @return none.
     */    
    public GeneralDBClient(String serverinfo_0, String serverinfo_1, String serverinfo_2, String cache_channel, String persist_channel)
    {
    	this.mCacheClient = new CacheLayerClient(serverinfo_0, serverinfo_1, serverinfo_2, cache_channel);
    	this.mPersistClient = new PersistentLayerClient(serverinfo_0, serverinfo_1, serverinfo_2, persist_channel);
    	
    	if((this.mCacheClient.CachaInitOK == true) && (this.mPersistClient.PersistInitOK == true))
    	{
    		this.allClientOK = true;
    	}
    	else
    	{
    		this.allClientOK = false;
    	}
    }
}
