package com.info7255.medicalplan.service;

import com.info7255.medicalplan.dao.MedicalPlanDAO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.info7255.medicalplan.Constants.Constants.*;


@Service
public class MedicalPlanService {
    @Autowired
    MedicalPlanDAO medicalPlanDAO;

    @Autowired
    MessageQueueService messageQueueService;

    public String saveMedicalPlan(JSONObject planObject, String key) {

        Map<String, Object> saveMedicalPlanMap = saveMedicalPlanToRedis(key, planObject);
        String saveMedicalPlanString = new JSONObject(saveMedicalPlanMap).toString();


        messageQueueService.publish(saveMedicalPlanString, MESSAGE_QUEUE_POST_OPERATION);

        String newEtag = DigestUtils.md5DigestAsHex(saveMedicalPlanString.getBytes(StandardCharsets.UTF_8));
        medicalPlanDAO.hSet(key, ETAG_KEY_NAME, newEtag);
        return newEtag;
    }



    public Map<String, Object> saveMedicalPlanToRedis(String key, JSONObject planObject) {
        saveJSONObjectToRedis(planObject);
        return getMedicalPlan(key);
    }

    public Map<String, Object> getMedicalPlan(String redisKey) {
        Map<String, Object> outputMap = new HashMap<>();

        Set<String> keys = medicalPlanDAO.getKeysByPattern(redisKey + REDIS_ALL_PATTERN);
        for (String key : keys) {
            if (key.equals(redisKey)) {
                Map<String, String> value = medicalPlanDAO.hGetAll(key);
                for (String name : value.keySet()) {
                    if (!name.equalsIgnoreCase(ETAG_KEY_NAME)) {
                        outputMap.put(name,
                                isDouble(value.get(name)) ? Double.parseDouble(value.get(name)) : value.get(name));
                    }
                }
            } else if (!MESSAGE_QUEUE_NAME.equals(key) && !WORKING_QUEUE_NAME.equals(key)) {
                String newKey = key.substring((redisKey + PRE_FIELD_DELIMITER).length());
                Set<String> members = medicalPlanDAO.sMembers(key);
                if (members.size() > 1) {
                    List<Object> listObj = new ArrayList<>();
                    for (String member : members) {
                        listObj.add(getMedicalPlan(member));
                    }
                    outputMap.put(newKey, listObj);
                } else {
                    Map<String, String> val = medicalPlanDAO.hGetAll(members.iterator().next());
                    Map<String, Object> newMap = new HashMap<>();
                    for (String name : val.keySet()) {
                        newMap.put(name,
                                isDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                    }
                    outputMap.put(newKey, newMap);
                }
            }
        }

        return outputMap;
    }

    public void deleteMedicalPlan(String redisKey) {
        Set<String> keys = medicalPlanDAO.getKeysByPattern(redisKey + REDIS_ALL_PATTERN);
        for (String key : keys) {
            if (key.equals(redisKey)) {
                medicalPlanDAO.deleteKeys(new String[]{key});
            } else {
                Set<String> members = medicalPlanDAO.sMembers(key);
                if (members.size() > 1) {
                    for (String member : members) {
                        deleteMedicalPlan(member);
                    }
                    medicalPlanDAO.deleteKeys(new String[]{key});
                } else {
                    medicalPlanDAO.deleteKeys(new String[]{members.iterator().next(), key});
                }
            }
        }
    }

    public boolean existsRedisKey(String key) {
        return medicalPlanDAO.existsKey(key);
    }

    public String getMedicalPlanEtag(String key) {
        return medicalPlanDAO.hGet(key, ETAG_KEY_NAME);
    }

    private Map<String, Map<String, Object>> saveJSONObjectToRedis(JSONObject object) {
        Map<String, Map<String, Object>> redisKeyMap = new HashMap<>();
        Map<String, Object> objectFieldMap = new HashMap<>();

        String redisKey = object.get(OBJECT_TYPE_NAME) + PRE_ID_DELIMITER + object.get(OBJECT_ID_MAME);
        for (String field : object.keySet()) {
            Object value = object.get(field);
            if (value instanceof JSONObject) {
                Map<String, Map<String, Object>> convertedValue = saveJSONObjectToRedis((JSONObject) value);
                medicalPlanDAO.sadd(redisKey + PRE_FIELD_DELIMITER + field,
                        convertedValue.entrySet().iterator().next().getKey());
            } else if (value instanceof JSONArray) {
                List<Map<String, Map<String, Object>>> convertedValue = saveJSONArrayToRedis((JSONArray) value);
                for (Map<String, Map<String, Object>> entry : convertedValue) {
                    for (String listKey : entry.keySet()) {
                        medicalPlanDAO.sadd(redisKey + PRE_FIELD_DELIMITER + field, listKey);
                    }
                }
            } else {
                medicalPlanDAO.hSet(redisKey, field, value.toString());
                objectFieldMap.put(field, value);
                redisKeyMap.put(redisKey, objectFieldMap);
            }
        }

        return redisKeyMap;
    }

    private List<Map<String, Map<String, Object>>> saveJSONArrayToRedis(JSONArray array) {
        List<Map<String, Map<String, Object>>> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                List<Map<String, Map<String, Object>>> convertedValue = saveJSONArrayToRedis((JSONArray) value);
                list.addAll(convertedValue);
            } else if (value instanceof JSONObject) {
                Map<String, Map<String, Object>> convertedValue = saveJSONObjectToRedis((JSONObject) value);
                list.add(convertedValue);
            }
        }
        return list;
    }

    private boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException numberFormatException) {
            return false;
        }
    }

    public Map<String, Object> savePlan(String key, JSONObject planObject){
        convertToMap(planObject);
        Map<String, Object> outputMap = new HashMap<String, Object>();
        getOrDeleteData(key, outputMap, false);
        return outputMap;
    }

    private Map<String, Map<String, Object>> convertToMap(JSONObject object) {
        Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();
        Map<String, Object> valueMap = new HashMap<String, Object>();

        Iterator<String> iterator = object.keySet().iterator();
        while (iterator.hasNext()) {
            System.out.println("Object 1 "+object);
            System.out.println("Object 2 "+object.get("objectType") );
            String redisKey = object.get("objectType") + "_" + object.get("objectId");
            String key = iterator.next();
            Object value = object.get(key);
            if (value instanceof JSONObject) {
                value = convertToMap((JSONObject) value);
                HashMap<String, Map<String, Object>> val = (HashMap<String, Map<String, Object>>) value;
                medicalPlanDAO.addSetValue(redisKey + "_" + key, val.entrySet().iterator().next().getKey());
            } else if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value);
                for (HashMap<String, HashMap<String, Object>> entry : (List<HashMap<String, HashMap<String, Object>>>) value) {
                    for (String listKey : entry.keySet()) {
                        medicalPlanDAO.addSetValue(redisKey + "_" + key, listKey);
                        System.out.println(redisKey + "_" + key + " : " + listKey);
                    }
                }
            } else {
                medicalPlanDAO.hSet(redisKey, key, value.toString());
                valueMap.put(key, value);
                map.put(redisKey, valueMap);
            }

        }
        System.out.println("MAP: " + map.toString());
        return map;
    }

    private List<Object> convertToList(JSONArray array) {
        List<Object> list = new ArrayList<Object>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = convertToMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    private Map<String, Object> getOrDeleteData(String redisKey, Map<String, Object> outputMap, boolean isDelete) {
        Set<String> keys = medicalPlanDAO.getKeys(redisKey + "*");
        for (String key : keys) {
            if (key.equals(redisKey)) {
                if (isDelete) {
                    medicalPlanDAO.deleteKeys(new String[] {key});
                } else {
                    Map<String, String> val = medicalPlanDAO.getAllValuesByKey(key);
                    for (String name : val.keySet()) {
                        if (!name.equalsIgnoreCase("eTag")) {
                            outputMap.put(name,
                                    isStringDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                    }
                }

            } else {
                String newStr = key.substring((redisKey + "_").length());
                System.out.println("Key to be searched :" +key+"--------------"+newStr);
                Set<String> members = medicalPlanDAO.sMembers(key);
                if (members.size() > 1) {
                    List<Object> listObj = new ArrayList<Object>();
                    for (String member : members) {
                        if (isDelete) {
                            getOrDeleteData(member, null, true);
                        } else {
                            Map<String, Object> listMap = new HashMap<String, Object>();
                            listObj.add(getOrDeleteData(member, listMap, false));

                        }
                    }
                    if (isDelete) {
                        medicalPlanDAO.deleteKeys(new String[] {key});
                    } else {
                        outputMap.put(newStr, listObj);
                    }

                } else {
                    if (isDelete) {
                        medicalPlanDAO.deleteKeys(new String[]{members.iterator().next(), key});
                    } else {
                        Map<String, String> val = medicalPlanDAO.getAllValuesByKey(members.iterator().next());
                        Map<String, Object> newMap = new HashMap<String, Object>();
                        for (String name : val.keySet()) {
                            newMap.put(name,
                                    isStringDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                        outputMap.put(newStr, newMap);
                    }
                }
            }
        }
        return outputMap;
    }

    public String savePlanToRedisAndMQ(JSONObject planObject, String key) {
        Map<String, Object> savedPlanMap = savePlan(key, planObject);
        String savedPlan = new JSONObject(savedPlanMap).toString();

        // save plan to MQ
        messageQueueService.addToMessageQueue(savedPlan, false);
        //String newEtag = DigestUtils.md5Hex(savedPlan);
        //medicalPlanDAO.hSet(key, "eTag", newEtag);

        String newEtag = DigestUtils.md5DigestAsHex(savedPlan.getBytes(StandardCharsets.UTF_8));
        medicalPlanDAO.hSet(key, ETAG_KEY_NAME, newEtag);
        return newEtag;
    }

    public Map<String, Object> getDeletePlan(String redisKey, Map<String, Object> outputMap, boolean isDelete) {
        Set<String> keys = medicalPlanDAO.getKeys(redisKey + "*");
        for (String key : keys) {
            if (key.equals(redisKey)) {
                if (isDelete) {
                    medicalPlanDAO.deleteKeys(new String[] {key});
                } else {
                    Map<String, String> val = medicalPlanDAO.getAllValuesByKey(key);
                    for (String name : val.keySet()) {
                        if (!name.equalsIgnoreCase("eTag")) {
                            outputMap.put(name, isDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                    }
                }
            } else {
                String str = key.substring((redisKey + "_").length());
                System.out.println("key is==> " + key);
                Set<String> membersSet = medicalPlanDAO.getAllMembers(key);
                System.out.println("memberset==> " + membersSet.size());
                System.out.println("memberset==> " + membersSet);



                if (membersSet.size() > 1) {
                    System.out.println("inside 3");
                    List<Object> listObj = new ArrayList<>();
                    for (String member : membersSet) {
                        System.out.println("member is " + member);
                        if (isDelete) {
                            getDeletePlan(member, null, true);
                        } else {
                            Map<String, Object> listMap = new HashMap<>();
                            listObj.add(getDeletePlan(member, listMap, false));
                            System.out.println("listObj is==>" + listObj);
                        }
                    }

                    if (isDelete) {
                        medicalPlanDAO.deleteKeys(new String[] {key});
                    } else {
                        outputMap.put(str, listObj);
                    }
                    System.out.println("inside output map is==>" + outputMap);
                } else {
                    System.out.println("inside 4");
                    if (isDelete) {
                        medicalPlanDAO.deleteKeys(new String[]{membersSet.iterator().next(), key});
                    } else {



                        Map<String, String> val = medicalPlanDAO.getAllValuesByKey(membersSet.iterator().next());
                        System.out.println("val is==> " + val);
                        Map<String, Object> newMap = new HashMap<>();
                        for (String name : val.keySet()) {
                            System.out.println("name is==> " + name);
                            newMap.put(name, isDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                        outputMap.put(str, newMap);
                        System.out.println("inside output map is1111==>" + outputMap);
                    }
                }
            }
        }
        System.out.println("OUTPUT===> " + outputMap);
        return outputMap;
    }









}
