package redis.clients.util;

import java.util.Random;

public class Ob
{
	Integer pkid=0;
	String name="";
	Integer age=0;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public Integer getPkid() {
		return pkid;
	}
	public void setPkid(Integer pkid) {
		this.pkid = pkid;
	}
	@Override
	public String toString() {
		return "{pkid:\""+this.pkid+"\",name:\""+this.name+"\",age:"+age+"}";
	}
	
}
