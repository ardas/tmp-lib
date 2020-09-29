package ua.ardas.esputnik.services.clients.razgrebator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.stereotype.Component;
import ua.ardas.esputnik.ms3.cassandra.domain.Interaction;
import ua.ardas.esputnik.redis.RedisNames;
import ua.ardas.esputnik.redis.dto.ComplaintInfo;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

@Component
public class RazgrebatorClient {

    @Autowired
    @Qualifier("redisTemplatePrototype")
    private RedisTemplate<String, ComplaintInfo> redisTemplate;
    private BlockingQueue<ComplaintInfo> resultsQueue;

    @PostConstruct
    private void init() {
        redisTemplate.setValueSerializer(new Jackson2JsonRedisSerializer<>(ComplaintInfo.class));
        resultsQueue = new DefaultRedisList<>(RedisNames.RESULTS_UNSUBSCRIBED, redisTemplate);
    }

    public void processSpam(int orgId, int contactId, Integer messageInstanceId, Long imId, String feedback, Integer contentPartId) throws InterruptedException {
        ComplaintInfo ir = new ComplaintInfo();
        ir.setMessageInstanceId(messageInstanceId);
        ir.setSpam(true);
        ir.setContactId(contactId);
        ir.setReason(feedback);
        ir.setOrgId(orgId);
        ir.setImId(imId);
        ir.setTimeStamp(new Date());
        ir.setContentPartId(contentPartId);

        resultsQueue.put(ir);
    }

    public void processUnsubscribed(Interaction interaction, String feedback) throws InterruptedException {
        ComplaintInfo ir = new ComplaintInfo(interaction.getInteractionId());
        ir.setOrgId(interaction.getOrganisationId());
        ir.setReason(feedback);
        ir.setTimeStamp(new Date());

        resultsQueue.put(ir);
    }

    public void processUnsubscribed(String iid, String feedback) throws InterruptedException {
        ComplaintInfo ir = new ComplaintInfo(iid);
        ir.setTimeStamp(new Date());
        ir.setReason(feedback);

        resultsQueue.put(ir);
    }

    public void processUnsubscribed(String iid) throws InterruptedException {
        processUnsubscribed(iid, null);
    }

    public void processUnsubscribed(int orgId, int contactId, Integer messageInstanceId, Long imId, String feedback) throws InterruptedException {
        ComplaintInfo ir = new ComplaintInfo();
        ir.setMessageInstanceId(messageInstanceId);
        ir.setContactId(contactId);
        ir.setReason(feedback);
        ir.setOrgId(orgId);
        ir.setImId(imId);
        ir.setTimeStamp(new Date());

        resultsQueue.put(ir);
    }
}
