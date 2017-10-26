package redis.clients.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

/**
 * 业务对象操作工具类
 * @author fyz
 */
public class ObjectUtil {
	/**
	 * 对象转化为 json字符串
	 * @param obj 对象实例
	 * @return json字符串
	 * @throws IOException
	 */
	public static String bean2Json(Object obj)
	{
        ObjectMapper mapper = new ObjectMapper();
        StringWriter sw=null;
		JsonGenerator gen=null;
		try {
			sw = new StringWriter();
			gen = new JsonFactory().createJsonGenerator(sw);
			mapper.writeValue(gen, obj);
			return sw.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		finally
		{
			try {
				gen.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
    }
	
	/**
	 * json字符串转化为单个类对象实例
	 * @param jsonStr json字符串
	 * @param objClass 对象类型
	 * @return 对象实例
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
    public static <T> T json2Bean(String jsonStr, Class<T> objClass)
    {
    	ObjectMapper mapper = new ObjectMapper();
        try {
			return mapper.readValue(jsonStr, objClass);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }
    
    /**
     * json字符串转化为集合类型：如List,Set等
     * @param jsonStr json字符串
     * @param collectionClass 集合类型
     * @param objClass 集合中对象类型
     * @return 集合对象实例
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static Object json2BeanCollection(String jsonStr,Class<?> collectionClass,Class<?> objClass)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
			JavaType javaType = getCollectionType(mapper, collectionClass, objClass);
			return mapper.readValue(jsonStr, javaType);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }
    /**
     * json字符串转化为Map类型
     * @param jsonStr json字符串
     * @param keyClass map key类型
     * @param valueClass map value类型
     * @return Map对象实例
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    static Object json2BeanMap(String jsonStr,Class<?> keyClass,Class<?> valueClass)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
			JavaType javaType = getMapType(mapper, keyClass, valueClass);
			return mapper.readValue(jsonStr, javaType);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }
    /**   
    * 获取泛型的Collection Type  
    * @param collectionClass 泛型的Collection   
    * @param elementClasses 元素类   
    * @return JavaType Java类型   
    */   
    static JavaType getCollectionType(ObjectMapper mapper,Class<?> collectionClass, Class<?> elementClasses) {   
       return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);   
    }
    /**   
    * 获取泛型的Map Type  
    * @param keyClass 泛型的key   
    * @param valueClass 元素类   
    * @return JavaType Java类型   
    */   
    static JavaType getMapType(ObjectMapper mapper,Class<?> keyClass, Class<?> valueClass) {   
       return mapper.getTypeFactory().constructMapType(Map.class, keyClass, valueClass);
    }
	public static void main(String[] args) throws IOException {
		Ob ob=new Ob();
		ob.setAge(22);
		ob.setName("fyz");
		String jsonStr=ObjectUtil.bean2Json(ob);
		Ob returnOb=ObjectUtil.json2Bean(jsonStr,Ob.class);
		System.out.println("json:"+jsonStr+",ob:"+returnOb);
		System.out.println("======================list");
		List<Ob> obs=new ArrayList<Ob>();
		obs.add(ob);
		obs.add(ob);
		String jsonStrList=ObjectUtil.bean2Json(obs);
		List<Ob> returnObs=(List<Ob>)ObjectUtil.json2BeanCollection(jsonStrList, List.class, Ob.class);
		System.out.println("json:"+jsonStrList+",obs:"+returnObs);
		System.out.println("======================map");
		Map<String,Ob> obsMap=new HashMap<String,Ob>();
		obsMap.put("1",ob);
		obsMap.put("2",ob);
		String jsonStrMap=ObjectUtil.bean2Json(obsMap);
		Map<String,Ob> returnObsMap=(Map<String,Ob>)ObjectUtil.json2BeanMap(jsonStrMap, String.class, Ob.class);
		System.out.println("json:"+jsonStrMap+",obs:"+returnObsMap);
	}

}
