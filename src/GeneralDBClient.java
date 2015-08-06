/**
 * @Copyright to Yonggang.Yang .
 * @ClassName: GeneralDBClient.
 * @Project: AutoShardingDBPlatform.
 * @Package: generaldbplatform.
 * @Description: The basic client of the auto sharding general database platform.
 * @Author: Yonggang.Yang 
 * @Version: V2.0
 * @Date: 2015-07-22
 * @History: 
 *    1.2015-07-21 First version of GeneralDBClient was written.
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
    
    

    
    
    public GeneralDBClient(String ip, String cache_channel, String persist_channel)
    {
    	this.mCacheClient = new CacheLayerClient(ip, cache_channel);
    	this.mPersistClient = new PersistentLayerClient(ip, persist_channel);
    	
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
