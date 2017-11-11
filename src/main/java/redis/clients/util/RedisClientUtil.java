package redis.clients.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis集群的操作工具类封装
 * @author fyz
 *
 */
public class RedisClientUtil {
	private static JedisPool pool; 					// 线程池对象
	private static String ADDR = "101.236.60.51"; 	// redis所在服务器地址（案例中是在本机）
	private static int PORT = 7000; 				// 端口号
	private static String AUTH = ""; 				// 密码（我没有设置）
	private static int MAX_IDLE = 10; 				// 连接池最大空闲连接数（最多保持空闲连接有多少）
	private static int MAX_ACTIVE = 50; 			// 最大激活连接数（能用的最多的连接有多少）
	private static int MAX_WAIT = 30000; 			// 等待可用连接的最大时间(毫秒)，默认值-1，表示永不超时。若超过等待时间，则抛JedisConnectionException
	private static int TIMEOUT = 10000; 			// 链接连接池的超时时间#使用连接时，检测连接是否成功
	private static boolean TEST_ON_BORROW = true; 	// 使用连接时，测试连接是否可用
	private static boolean TEST_ON_RETURN = true; 	//返回连接时，测试连接是否可用
	public static Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
	public static JedisCluster jc = null;
	
	/**
	 * 加载外部配置文件
	 */
	public static void loadConfig(String... path)
	{
		try
		{
			Properties pro=new Properties();
			InputStream inStream=null;
			if(path==null)
			{//如果未指定，默认认为当前是Web集成方式，那么只需要加载classes目录下的redis-cluster-config.properties即可
				System.out.println("WEB项目启动时加载redis配置文件路径：WEB-INF/classes/redis-cluster-config.properties");
				inStream=RedisClientUtil.class.getClassLoader().getResourceAsStream("/redis-cluster-config.properties");
			}else
			{//如果指定，默认认为是加载与主jar包所在的目录下的redis-cluster-config.properties即可
				File configFile=new File(path[0]);
				System.out.println("JAR项目启动时加载redis配置文件路径："+configFile.getAbsolutePath());
				inStream=new FileInputStream(configFile);
			}
			pro.load(inStream);
			String master_ip=pro.getProperty("master_ip");
			String master_port=pro.getProperty("master_port");
			String max_idle=pro.getProperty("max_idle");
			String max_active=pro.getProperty("max_active");
			String max_wait=pro.getProperty("max_wait");
			String timeout=pro.getProperty("timeout");
			if(master_ip!=null&&!"".equals(master_ip))
			{
				ADDR=master_ip;
			}
			if(master_port!=null&&!"".equals(master_port))
			{
				PORT=Integer.parseInt(master_port);
			}
			if(max_idle!=null&&!"".equals(max_idle))
			{
				MAX_IDLE=Integer.parseInt(max_idle);
			}
			if(max_active!=null&&!"".equals(max_active))
			{
				MAX_ACTIVE=Integer.parseInt(max_active);
			}
			if(max_wait!=null&&!"".equals(max_wait))
			{
				MAX_WAIT=Integer.parseInt(max_wait);
			}
			if(timeout!=null&&!"".equals(timeout))
			{
				TIMEOUT=Integer.parseInt(timeout);
			}
		}catch(Exception e){e.printStackTrace();}
	}
	private RedisClientUtil()
	{
		/**
		 * 由于集群的自动发现和连接性，所以，只需要连接集群中的一个节点即可
		 */
		HostAndPort clusterNode1=new HostAndPort(ADDR, PORT);
		/*
		HostAndPort clusterNode2=new HostAndPort("127.0.0.1", 7001);
		HostAndPort clusterNode3=new HostAndPort("127.0.0.1", 7002);
		HostAndPort clusterNode4=new HostAndPort("127.0.0.1", 7003);
		HostAndPort clusterNode5=new HostAndPort("127.0.0.1", 7004);
		HostAndPort clusterNode6=new HostAndPort("127.0.0.1", 7005);
		*/
		JedisPoolConfig config = new JedisPoolConfig();
	    config.setMaxIdle(MAX_IDLE);
	    config.setMaxTotal(MAX_ACTIVE);
	    config.setMaxWaitMillis(MAX_WAIT);
	    config.setTestOnBorrow(TEST_ON_BORROW);  
	    config.setTestOnReturn(TEST_ON_RETURN);
	    pool = new JedisPool(config, ADDR, PORT, TIMEOUT); //新建连接池，如有密码最后加参数 
		jedisClusterNodes.add(clusterNode1);
		/*
		jedisClusterNodes.add(clusterNode2);
		jedisClusterNodes.add(clusterNode3);
		jedisClusterNodes.add(clusterNode4);
		jedisClusterNodes.add(clusterNode5);
		jedisClusterNodes.add(clusterNode6);
		*/
		jc = new JedisCluster(jedisClusterNodes,config);
	}
	private static RedisClientUtil instance=null;
	public static RedisClientUtil getInstance()
	{
		if(instance==null)
		{
			instance=new RedisClientUtil();
		}
		return instance;
	}

	/**
	 * 单个字符串内容设置
	 * @param key 键值
	 * @param str 字符串内容
	 */
	public static void set(String key, String str) {
//		Date start=new Date();
		try {
			jc.set(key, str);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 单个字符串内容获取
	 * @param key 键值
	 * @param str 字符串内容
	 */
	public static String get(String key) {
//		Date start=new Date();
		try {
			String value=jc.get(key);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key:"+key+",value:"+value+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * map内容设置
	 * @param key 键值
	 * @param value 内容
	 */
	public static void setMapByKey(String mapName, String key,String value) {
//		Date start=new Date();
		try {
			jc.hset(mapName, key,value);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}	
	
	
	/**
	 * map内容设置
	 * @param key 键值
	 * @param value 内容
	 */
	public static void setMap(String mapName,Map<String,String> map) {
//		Date start=new Date();
		try {
			jc.hmset(mapName,map);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	
	/**
	 * map内容设置
	 * @param mapName map名称
	 * @param value 内容
	 */
	public static Map<String,String> getMap(String mapName) {
//		Date start=new Date();
		try {
			Map<String,String> map = jc.hgetAll(mapName);
			return map;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取单个map 指定key的值
	 * @param mapName map名称
	 */
	public static String getMapValueByKey(String mapName,String key) {
//		Date start=new Date();
		try {
			String value = jc.hget(mapName, key);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取列表map 指定keys的 值列表
	 * @param mapName map名称
	 */
	public static List<String> getMapValuesByKeys(String mapName,String... keys) {
//		Date start=new Date();
		try {
			List<String> list = jc.hmget(mapName, keys);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取map 所有keys列表
	 * @param mapName map名称
	 */
	public static Set<String> getMapKeys(String mapName) {
//		Date start=new Date();
		try {
			Set<String> set = jc.hkeys(mapName);
			return set;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取map 所有value列表
	 * @param mapName map名称
	 */
	public static List<String> getMapValues(String mapName) {
//		Date start=new Date();
		try {
			List<String> list = jc.hvals(mapName);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * map内容删除
	 * @param mapName map名称
	 * @param keys 删除的键值对列表
	 */
	public static Long deleteMapByKey(String mapName,String... keys) {
//		Date start=new Date();
		try {
			Long  num= jc.hdel(mapName, keys);
			return num;
		} catch (Exception e) {
			e.printStackTrace();
			return 0l;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * map中的key是否存在
	 * @param mapName map名称
	 * @param key 键值
	 * @param 是否存在
	 */
	public static Boolean mapExists(String mapName,String key) {
//		Date start=new Date();
		try {
			Boolean isExisted=jc.hexists(mapName,key);
			return isExisted;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
//		Date end=new Date();
//		System.out.println("key:"+key+",value:"+value+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * map中的长度
	 * @param mapName map名称
	 * @param 长度
	 */
	public static Long mapLen(String mapName) {
//		Date start=new Date();
		try {
			Long mapLen=jc.hlen(mapName);
			return mapLen;
		} catch (Exception e) {
			e.printStackTrace();
			return 0l;
		}
//		Date end=new Date();
//		System.out.println("key:"+key+",value:"+value+",time:"+(end.getTime()-start.getTime()));
	}
	
	
	/**
	 * 从右侧list添加内容
	 * @param listName 列表集合名称
	 * @param values 内容字符串
	 */
	public static Long setListValueFromRight(String listName,String... values) {
//		Date start=new Date();
		try {
			Long num=jc.rpush(listName,values);
			return num;
		} catch (Exception e) {
			e.printStackTrace();
			return 0l;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 从左侧list添加内容
	 * @param listName 列表集合名称
	 * @param values 内容字符串
	 */
	public static Long setListValueFromLeft(String listName,String... values) {
//		Date start=new Date();
		try {
			Long num=jc.lpush(listName,values);
			return num;
		} catch (Exception e) {
			e.printStackTrace();
			return 0l;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取list内容列表
	 * @param listName 列表集合名称
	 * @param start 开始位置默认为0
	 * @param end -1 结束位置默认为最后一个
	 */
	public static List<String> getListAll(String listName) {
//		Date start=new Date();
		try {
			List<String> list=jc.lrange(listName,0,-1);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取list内容列表
	 * @param listName 列表集合名称
	 * @param start 开始位置
	 * @param end -1代表从后往前最后一个
	 */
	public static List<String> getList(String listName,int start,int end) {
//		Date start=new Date();
		try {
			List<String> list=jc.lrange(listName, start, end);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	
	/**
	 * 从左侧删除list第一个元素
	 * @param listName 列表集合名称
	 */
	public static String deleteListFromLeft(String listName) {
//		Date start=new Date();
		try {
			String result=jc.lpop(listName);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 从右侧删除list第一个元素
	 * @param listName 列表集合名称
	 */
	public static String deleteListFromRight(String listName) {
//		Date start=new Date();
		try {
			String result=jc.rpop(listName);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	
	/**
	 * 获取list内容列表长度
	 * @param listName 列表集合名称
	 * @return 长度
	 */
	public static Long listLen(String listName) {
//		Date start=new Date();
		try {
			Long num=jc.llen(listName);
			return num;
		} catch (Exception e) {
			e.printStackTrace();
			return 0l;
		}
//		Date end=new Date();
//		System.out.println("key-" + key + " slot-" + JedisClusterCRC16.getSlot(key) + " value-" + value);
//		System.out.println("key:"+key+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * 获取匹配模式的keys：支持*，举例：如order_*
	 * @param keys_pat 匹配表达式
	 * @param 匹配的key列表
	 */
	public static Set<String> findKeys(String pattern) {
        Set<String> keys = new HashSet<String>();  
        Map<String, JedisPool> clusterNodes = jc.getClusterNodes();  
        for(String k : clusterNodes.keySet()){  
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try
            {
            	keys.addAll(connection.keys(pattern));  
            }
            catch(Exception e)
            {
            	e.printStackTrace();
            }finally
            { 
                connection.close();//用完一定要close这个链接！！！  
            }
        }
        return keys;
	}
	/**
	 * 要删除的keys
	 * @param keys 键值
	 * @param 删除的key数量
	 */
	public static Long delete(String... keys) {
//		Date start=new Date();
		try {
			Long deleteKeys=jc.del(keys);
			return deleteKeys;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
//		Date end=new Date();
//		System.out.println("key:"+key+",value:"+value+",time:"+(end.getTime()-start.getTime()));
	}
	/**
	 * key是否存在
	 * @param key 键值
	 * @param 是否存在
	 */
	public static Boolean exists(String key) {
//		Date start=new Date();
		try {
			Boolean isExisted=jc.exists(key);
			return isExisted;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
//		Date end=new Date();
//		System.out.println("key:"+key+",value:"+value+",time:"+(end.getTime()-start.getTime()));
	}
	public static void main(String[] args) {
		/**
		 * 1.从当前项目加载配置文件
		 */
		RedisClientUtil.loadConfig("redis-cluster-config.properties");
		/**
		 * 2.创建实例
		 */
		RedisClientUtil.getInstance();
		String keyStr="order.id.{123}";
		RedisClientUtil.set(keyStr,"{name:'fyz',age:19}");
		System.out.println(RedisClientUtil.get(keyStr));
		if(1==1){return;}
		//==删除测试
//		for(int i=0;i<4;i++)
//		{
//			redisClient.deleteListFromLeft("name");
//			redisClient.deleteListFromRight("name");
//		}
//		if(1==1){return;}
		//==添加测试
//		Ob ob=new Ob();
//		ob.setAge(32);
//		ob.setName("fyz");
//		String ob_json=ObjectUtil.bean2Json(ob);
//		RedisClientUtil.setListValueFromRight("name",ob_json);
		//==list类型变量读取
//		long start1 = System.currentTimeMillis();
//		List<String> list=RedisClientUtil.getListAll("name");
//		 long end1 = System.currentTimeMillis();
//		 System.out.println("redis读数据耗时：" + (end1 - start1));
//		System.out.println("list:"+list);
//		System.out.println("list len:"+RedisClientUtil.listLen("name"));
//		for(String str:list){
//            System.out.println("[获取对象数据]：" + str);
//            Ob get_ob=ObjectUtil.json2Bean(str,Ob.class);
//            System.out.println("ob：" + get_ob.toString());
//        }
//		if(1==1){return;}
		//==测试设置键
//		RedisClientUtil.set("name1",new Random().nextInt(100)+"");
		//==测试删除键
//		RedisClientUtil.delete(new String[]{"name1"});
//		String value1=RedisClientUtil.get("name1");
//		String value2=RedisClientUtil.get("name2");
//		System.out.println("key1 value:"+value1+","+",key2:"+value2);
		//==测试Map相关操作 key相同的会被覆盖更新
		Map<String,String> userMap=new HashMap<String,String>();
		Ob m1_ob=new Ob();m1_ob.setPkid(1);m1_ob.setAge(21);m1_ob.setName("abc");
		Ob m1_ob_update=new Ob();m1_ob_update.setPkid(1);m1_ob_update.setAge(22);m1_ob_update.setName("abc1");
		Ob m2_ob=new Ob();m2_ob.setPkid(2);m2_ob.setAge(22);m2_ob.setName("def");
		
		userMap.put(m1_ob.getPkid()+"", ObjectUtil.bean2Json(m1_ob));
		userMap.put(m2_ob.getPkid()+"", ObjectUtil.bean2Json(m2_ob));
		//一次更新多个map中的key value
		RedisClientUtil.setMap("userMap", userMap);
		//一次更新一个map中的key的value
		RedisClientUtil.setMapByKey("userMap", m1_ob_update.getPkid()+"",ObjectUtil.bean2Json(m1_ob_update));
		//返回整个Map表
		Map<String,String> get_userMap=RedisClientUtil.getMap("userMap");
		System.out.println("userMap:"+get_userMap);
		//返回 map中的单条数据
		String return_m1_ob_json=RedisClientUtil.getMapValueByKey("userMap",m1_ob.getPkid()+"");
		System.out.println("userObj:"+ObjectUtil.json2Bean(return_m1_ob_json, Ob.class));
		//查找所有库中的key
		Set<String> keys=RedisClientUtil.findKeys("*");
		for(String key:keys)
		{
			//判断key的类型
			System.out.println("key type:"+RedisClientUtil.jc.type(key));
		}
		System.out.println("keys:"+keys);
		/*
        for(String str:list1){
            System.out.println("[获取对象数据]：" + str);
            Ob ob=ObjectUtil.json2Bean(str,Ob.class);
            System.out.println("ob：" + ob.toString());
        }
		if(1==1)return;
		*/
		/*
		int threadnumber=10000;
		CyclicBarrier barrier=new CyclicBarrier(threadnumber);
		ExecutorService executor=Executors.newFixedThreadPool(threadnumber);
		for(int i=0;i<threadnumber;i++)
		{
			TestSetThread t=new TestSetThread(barrier,redisClient); 
			executor.submit(t);
		}
		executor.shutdown();
		while(!executor.isTerminated())
		{
			System.out.println("休眠===");
			try {
				Thread.sleep(50);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("====测试结束=");
		executor.shutdownNow();
		
		long start2 = System.currentTimeMillis();
        List<String> list2 = redisClient.jc.lrange("name", 0, -1);
        long end2 = System.currentTimeMillis();
        System.out.println("redis读数据耗时：" + (end2 - start2));
        System.out.println("内容："+list2);
        
        for(String str:list2){
            System.out.println("[获取对象数据]：" + str);
            Ob ob=ObjectUtil.json2Bean(str,Ob.class);
            System.out.println("ob：" + ob.toString());
        }
        */
	}

}
class TestSetThread extends Thread
{
	private CyclicBarrier barrier;
	private RedisClientUtil client;
	public TestSetThread(CyclicBarrier barrier,RedisClientUtil client)
	{
		this.barrier=barrier;
		this.client=client;
	}
	@Override
	public void run() {
//		client.set("name","abc");
//		client.get("name");
		Ob ob=new Ob();
		ob.setAge(100);
		ob.setName("peter");
		String ob_json=ObjectUtil.bean2Json(ob);
		if(ob_json!=null)
		{
			client.setListValueFromLeft("name",ob_json);
		}
		try {
			if(barrier!=null)
			{
				barrier.await();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(Thread.currentThread().getName()+"===finished");
	}
	
}
