package org.globex.retail.streams.serde;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class FixedSizePriorityQueueSerDeTest {

    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        Comparator<ProductScore> comparator = (pl1, pl2) -> pl2.getScore() - pl1.getScore();
        FixedSizePriorityQueue<ProductScore> queue = new FixedSizePriorityQueue<>(comparator, 5);
        queue.add(new ProductScore.Builder("123456").category("cat123456").score(10).build());
        queue.add(new ProductScore.Builder("234567").category("cat234567").score(5).build());
        queue.add(new ProductScore.Builder("345678").category("cat345678").score(15).build());
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        FixedSizePriorityQueueSerializer serializer = new FixedSizePriorityQueueSerializer();
        module.addSerializer(FixedSizePriorityQueue.class, serializer);
        mapper.registerModule(module);
        String serialized = mapper.writeValueAsString(queue);

        mapper = new ObjectMapper();
        module = new SimpleModule();
        FixedSizePriorityQueueDeserializer deserializer = new FixedSizePriorityQueueDeserializer(comparator, 10);
        module.addDeserializer(FixedSizePriorityQueue.class, deserializer);
        mapper.registerModule(module);

        FixedSizePriorityQueue<ProductScore> fromJson = mapper.readValue(serialized, FixedSizePriorityQueue.class);
        MatcherAssert.assertThat(fromJson.size(), Matchers.equalTo(3));
        List<String> products = new ArrayList<>();
        List<Integer> likes = new ArrayList<>();
        List<String> categories = new ArrayList<>();
        Iterator<ProductScore> i = fromJson.iterator();
        while (i.hasNext()) {
            ProductScore p = i.next();
            products.add(p.getProductId());
            likes.add(p.getScore());
            categories.add(p.getCategory());
        }
        MatcherAssert.assertThat(products.get(0), Matchers.equalTo("345678"));
        MatcherAssert.assertThat(products.get(1), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(products.get(2), Matchers.equalTo("234567"));
        MatcherAssert.assertThat(likes.get(0), Matchers.equalTo(15));
        MatcherAssert.assertThat(likes.get(1), Matchers.equalTo(10));
        MatcherAssert.assertThat(likes.get(2), Matchers.equalTo(5));
        MatcherAssert.assertThat(categories.get(0), Matchers.equalTo("cat345678"));
        MatcherAssert.assertThat(categories.get(1), Matchers.equalTo("cat123456"));
        MatcherAssert.assertThat(categories.get(2), Matchers.equalTo("cat234567"));
    }

}
