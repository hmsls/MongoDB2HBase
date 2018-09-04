package UTIL;

import com.mongodb.util.JSON;

import java.util.HashMap;
import java.util.Map;


/**
 *	json字符串转换为map对象
 */
public class JsonStrToMap {
	public static Map<Object, Object> jsonStrToMap(String jsonString) {
        Object parseObj = JSON.parse(jsonString); // 反序列化 把json 转化为对象
        Map<Object, Object> map = (HashMap<Object, Object>) parseObj; // 把对象转化为map
        return map;
    }
}
