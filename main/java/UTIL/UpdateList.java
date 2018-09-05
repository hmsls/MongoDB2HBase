package UTIL;

import java.util.ArrayList;
import java.util.List;

/**
 * 这个类的作用是把嵌套字段的每个字段所在的路径的编号加到了每个字段名字上，便于区分
 * 比如，/orgmsg下面的字段前缀（路径编号）为“o”，/orgmsg/detailQueryReason的字段
 * 前缀（路径编号）为“odqr”
 */
public class UpdateList {
    public static List<List<Object>> updateListElement(List<List<Object>> list){
        for(List<Object> l :  list){
            //判断列表元素个数是否大于1，只有1个元素的话，那就是只有一个路径
            if(l.size()>1){
                //得到每个列表的第一个元素，即路径元素
                String first = l.get(0).toString();
                //判断第一个元素不为空，因为初始父元素传的是“”
                if(!first.equals("")){
                    //根据“/”来切割第一个元素
                    String[] arg = first.split("/");
                    //创建字段路径前置的字符串
                    StringBuilder sb = new StringBuilder();
                    for(int k = 0;k<arg.length;k++){
                        //如果是数字，则直接拼接上去
                        if((arg[k]).matches("^[0-9]*$")){
                            sb.append(arg[k]);
                        }else{
                            //选取路径名称的第一个字母以及驼峰命名的大写字母
                            sb = sb.append(arg[k].charAt(0));
                            for(int r=1;r<arg[k].length();r++){
                                char c = arg[k].charAt(r);
                                if(c >= 'A' && c <= 'Z'){
                                    sb.append(c);
                                }
                            }
                        }
                    }
                    //为这个路径下的所有字段添加路径前缀
                    for(int i=1;i<l.size();i++){
                        l.set(i,sb.toString()+"_"+l.get(i));
                    }
                }
            }
        }
        return list;
    }
}
