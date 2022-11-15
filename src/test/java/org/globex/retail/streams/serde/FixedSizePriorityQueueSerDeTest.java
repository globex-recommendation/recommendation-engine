package org.globex.retail.streams.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FixedSizePriorityQueueSerDeTest {

    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        FixedSizePriorityQueue<ProductScore> queue = new FixedSizePriorityQueue<>(ProductScore.SCORE_ORDER, 5);
        queue.add(new ProductScore.Builder("123456").score(10).build());
        queue.add(new ProductScore.Builder("234567").score(5).build());
        queue.add(new ProductScore.Builder("345678").score(15).build());
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        FixedSizePriorityQueueSerializer serializer = new FixedSizePriorityQueueSerializer();
        module.addSerializer(FixedSizePriorityQueue.class, serializer);
        mapper.registerModule(module);
        String serialized = mapper.writeValueAsString(queue);

        mapper = new ObjectMapper();
        module = new SimpleModule();
        FixedSizePriorityQueueDeserializer deserializer = new FixedSizePriorityQueueDeserializer(ProductScore.SCORE_ORDER, 10);
        module.addDeserializer(FixedSizePriorityQueue.class, deserializer);
        mapper.registerModule(module);

        FixedSizePriorityQueue<ProductScore> fromJson = mapper.readValue(serialized, FixedSizePriorityQueue.class);
        MatcherAssert.assertThat(fromJson.size(), Matchers.equalTo(3));
        List<String> products = new ArrayList<>();
        List<Integer> likes = new ArrayList<>();
        Iterator<ProductScore> i = fromJson.iterator();
        while (i.hasNext()) {
            ProductScore p = i.next();
            products.add(p.getProductId());
            likes.add(p.getScore());
        }
        MatcherAssert.assertThat(products.get(0), Matchers.equalTo("345678"));
        MatcherAssert.assertThat(products.get(1), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(products.get(2), Matchers.equalTo("234567"));
        MatcherAssert.assertThat(likes.get(0), Matchers.equalTo(15));
        MatcherAssert.assertThat(likes.get(1), Matchers.equalTo(10));
        MatcherAssert.assertThat(likes.get(2), Matchers.equalTo(5));
    }

    @Test
    public void testSerializeAndDeserializeWithEqualScore() throws JsonProcessingException {
        FixedSizePriorityQueue<ProductScore> queue = new FixedSizePriorityQueue<>(ProductScore.SCORE_ORDER, 5);
        queue.add(new ProductScore.Builder("123456").score(5).build());
        queue.add(new ProductScore.Builder("234567").score(5).build());
        queue.add(new ProductScore.Builder("345678").score(5).build());
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        FixedSizePriorityQueueSerializer serializer = new FixedSizePriorityQueueSerializer();
        module.addSerializer(FixedSizePriorityQueue.class, serializer);
        mapper.registerModule(module);
        String serialized = mapper.writeValueAsString(queue);

        mapper = new ObjectMapper();
        module = new SimpleModule();
        FixedSizePriorityQueueDeserializer deserializer = new FixedSizePriorityQueueDeserializer(ProductScore.SCORE_ORDER, 10);
        module.addDeserializer(FixedSizePriorityQueue.class, deserializer);
        mapper.registerModule(module);

        FixedSizePriorityQueue<ProductScore> fromJson = mapper.readValue(serialized, FixedSizePriorityQueue.class);
        MatcherAssert.assertThat(fromJson.size(), Matchers.equalTo(3));
        List<String> products = new ArrayList<>();
        List<Integer> likes = new ArrayList<>();
        Iterator<ProductScore> i = fromJson.iterator();
        while (i.hasNext()) {
            ProductScore p = i.next();
            products.add(p.getProductId());
            likes.add(p.getScore());
        }
        MatcherAssert.assertThat(products.get(0), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(products.get(1), Matchers.equalTo("234567"));
        MatcherAssert.assertThat(products.get(2), Matchers.equalTo("345678"));
        MatcherAssert.assertThat(likes.get(0), Matchers.equalTo(5));
        MatcherAssert.assertThat(likes.get(1), Matchers.equalTo(5));
        MatcherAssert.assertThat(likes.get(2), Matchers.equalTo(5));
    }

}
