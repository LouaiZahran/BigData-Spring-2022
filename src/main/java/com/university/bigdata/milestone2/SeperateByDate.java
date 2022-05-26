package com.university.bigdata.milestone2;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SeperateByDate {
    public void rearrangeByDay(String filename){
        JSONParser parser = new JSONParser();
        try {

            String input= Files.readString(Path.of(filename));
            String[] res= tokenize(input);
            List<JSONObject> jsonArray=new ArrayList<>();
            for(Object i:res){
                    Object object=parser.parse((String) i);
                    jsonArray.add((JSONObject) object);
            }
            convertStampToDay(jsonArray);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
    private void convertStampToDay(List<JSONObject> objlist){
        List<Integer> existing_days=new ArrayList<>();
        for(JSONObject obj:objlist){
            boolean append=false;
            int time_in_millis=(int)obj.get("Timestamp");
            int day_number= Math.round(time_in_millis/(1000*3600*24));
            if(existing_days.contains(day_number)){
                append=true;
            }
            else{
                existing_days.add(day_number);
            }
            try {
                FileWriter writer =new FileWriter("healthday"+day_number+".json",append);
                writer.write(obj.toJSONString());
                writer.flush();
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private static String[] tokenize(String line){
        List<String> ret = new ArrayList<>();
        String[] messages = line.toString().split("}}");
        for (String message : messages) {
            if(!message.equals("\n")){
                message = message.concat("}}");
                ret.add(message);
            }

        }
        return ret.toArray(new String[0]);
    }
}
