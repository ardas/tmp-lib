package ua.ardas.esputnik.services.clients.razgrebator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.stereotype.Component;
import ua.ardas.esputnik.ms4.config.MS4RedisNames;
import ua.ardas.esputnik.redis.dto.ComplaintInfo;
import ua.ardas.esputnik.redis.dto.InteractionResult;

import javax.annotation.PostConstruct;
import java.util.concurrent.BlockingQueue;

@Component
public class MS4RazgrebatorClient {
    @Autowired
    @Qualifier("redisTemplatePrototype")
    private RedisTemplate<String, InteractionResult> redisTemplate;
    private BlockingQueue<InteractionResult> resultsQueue;

    @PostConstruct
    private void init() {
        redisTemplate.setValueSerializer(new Jackson2JsonRedisSerializer<>(ComplaintInfo.class));
        resultsQueue = new DefaultRedisList<>(MS4RedisNames.RESULTS_QUEUE, redisTemplate);
    }

    public void addResult(InteractionResult ir) throws InterruptedException {
        this.resultsQueue.put(ir);
    }
}
