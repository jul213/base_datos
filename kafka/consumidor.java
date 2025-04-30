import java.util.Properties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.consumerRecord;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class Consumer {

    static class ConsumerRebalance implements ConsumerRebalanceListener {
        public List<TopicPartition> assignedPartitions = new ArrayList<>();

        @Override
        
    }
}
