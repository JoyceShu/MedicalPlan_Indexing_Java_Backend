package com.info7255.medicalplan.service;

import com.info7255.medicalplan.dao.MedicalPlanDAO;
import com.info7255.medicalplan.dao.MessageQueueDao;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.info7255.medicalplan.Constants.Constants.*;


@Service
public class MessageQueueService {
    @Autowired
    MedicalPlanDAO medicalPlanDAO;

    @Autowired
    private MessageQueueDao messageQueueDao;

    public void publish(String message, String operation) {
        JSONObject object = new JSONObject();
        object.put("message", message);
        object.put("operation", operation);

        medicalPlanDAO.lpush(MESSAGE_QUEUE_NAME, object.toString());
    }

    public void addToMessageQueue(String message, boolean isDelete) {
        JSONObject object = new JSONObject();
        object.put("message", message);
        object.put("isDelete", isDelete);

        // save plan to message queue "messageQueue"
        messageQueueDao.addToQueue("messageQueue", object.toString());
        System.out.println("Message saved successfully: " + object.toString());
    }
}
