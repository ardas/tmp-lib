package ua.ardas.esputnik.services.clients.razgrebator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.stereotype.Component;
import ua.ardas.esputnik.ms3.cassandra.domain.Interaction;
import ua.ardas.esputnik.redis.RedisNames;
import ua.ardas.esputnik.redis.dto.UnsubscriptionResult;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

@Component
public class RazgrebatorClient {

    @Autowired
    @Qualifier("redisTemplatePrototype")
    private RedisTemplate<String, UnsubscriptionResult> redisTemplate;
    private BlockingQueue<UnsubscriptionResult> resultsQueue;

    @PostConstruct
    private void init() {
        redisTemplate.setValueSerializer(new Jackson2JsonRedisSerializer<UnsubscriptionResult>(UnsubscriptionResult.class));
        resultsQueue = new DefaultRedisList<UnsubscriptionResult>(RedisNames.RESULTS_UNSUBSCRIBED, redisTemplate);
    }

    public void processUnsubscribed(Interaction interaction, String feedback) throws InterruptedException {
        UnsubscriptionResult ir = new UnsubscriptionResult(interaction.getInteractionId());
        ir.setOrgId(interaction.getOrganisationId());
        ir.setReason(feedback);
        ir.setTimeStamp(new Date());

        resultsQueue.put(ir);
    }

    public void processUnsubscribed(String iid, String feedback) throws InterruptedException {
        UnsubscriptionResult ir = new UnsubscriptionResult(iid);
        ir.setTimeStamp(new Date());
        ir.setReason(feedback);

        resultsQueue.put(ir);
    }

    public void processUnsubscribed(String iid) throws InterruptedException {
        processUnsubscribed(iid, null);
    }

    public void processUnsubscribed(int orgId, int contactId, Integer messageInstanceId, Integer imId, String feedback) throws InterruptedException {
        UnsubscriptionResult ir = new UnsubscriptionResult();
        ir.setMessageInstanceId(messageInstanceId);
        ir.setContactId(contactId);
        ir.setReason(feedback);
        ir.setOrgId(orgId);
        ir.setImId(imId);
        ir.setTimeStamp(new Date());

        resultsQueue.put(ir);
    }

}
