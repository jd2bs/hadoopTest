package com.cisco.webex.bigdata.kaifang;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class StaticHelper {
	private static final transient Log LOG = LogFactory.getLog(StaticHelper.class);
	
	void addListener(){
		
	}
	
	void getActivities(){
		
	}

	public static <T> String getJsonString(T object){
		ObjectMapper mapper = new ObjectMapper();
		String json_str = null;
		try {
			json_str = mapper.writeValueAsString(object);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return json_str;
	}
}
