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
                    //判断/orgmsg类型
                    if(arg[1].equals("orgmsg") && (arg.length)==2){
                        //重新设置元素值
                        String newOne = "o";
                        l.set(0,newOne);
                        for(int i=1;i<l.size();i++){
                            l.set(i,newOne+"_"+l.get(i));
                        }
                    }
                    if(arg.length > 2){
                        //判断/orgmsg/detailQueryReason类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("detailQueryReason")){
                            //重新设置元素值
                            String newOne = "odqr"+arg[3];
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/creditSummaryCue类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("creditSummaryCue")){
                            //重新设置元素值
                            String newOne = "ocsc";
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/identity类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("identity")){
                            //重新设置元素值
                            String newOne = "oi";
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/messageHeader类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("messageHeader")){
                            //重新设置元素值
                            String newOne = "omh";
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/numAnalysis类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("numAnalysis")){
                            //重新设置元素值
                            String newOne = "ona";
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/professional/类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("professional")){
                            //重新设置元素值
                            String newOne = "op"+arg[3];
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/queryReq类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("queryReq")){
                            //重新设置元素值
                            String newOne = "oqr";
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/recordDetail/类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("recordDetail")){
                            //重新设置元素值
                            String newOne = "ord"+arg[3];
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/residence/类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("residence")){
                            //重新设置元素值
                            String newOne = "or"+arg[3];
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                        //判断/orgmsg/spouse/类型
                        if(arg[1].equals("orgmsg") && arg[2].equals("spouse")){
                            //重新设置元素值
                            String newOne = "os"+arg[3];
                            l.set(0,newOne);
                            for(int i=1;i<l.size();i++){
                                l.set(i,newOne+"_"+l.get(i));
                            }
                        }
                    }
                }
            }
        }
        return list;
    }
}
