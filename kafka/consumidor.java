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
        public synchronized void onPartitionsAssigned(collection<TopicPartition> partitions){
            System.out.println("assigned partitions");
            for (TopicPartition tp: partitions ){
                System.out.println(tp);
                assignedPartitions.add(tp);
            }
        }

        @Override
		public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println("Revoked previously assigned partitions. ");
			for (TopicPartition tp :assignedPartitions ) {
				System.out.println(tp);
			}
			assignedPartitions.clear();
		}
	}

	public static void main(String[] args) {
		//System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
		Properties props = new Properties();
		//IP or Host name where Oracle Database 23ai is running and Database Listener's Port
		props.put("bootstrap.servers", "localhost:1521");
		
		//name of the service running on the database instance
		props.put("oracle.service.name", "freepdb1");
		props.put("security.protocol","PLAINTEXT");
		
		// location for ojdbc.properties file where user and password properties are saved
		props.put("oracle.net.tns_admin","."); 
		
		//Consumer Group Name
		props.put("group.id" , "CG1");
		props.put("enable.auto.commit","false");
		
		// Maximum number of records fetched in single poll call
		props.put("max.poll.records", 2000);

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<String , String> consumer = new KafkaConsumer<String, String>(props);		
		ConsumerRebalanceListener rebalanceListener = new ConsumerRebalance();
		
		//Subscribe to a single topic named 'TEQ'.
		consumer.subscribe(Arrays.asList("TEQ"), rebalanceListener);
		
		int expectedMsgCnt = 40000;
		int msgCnt = 0;
		Instant startTime = Instant.now();
		try {
			while(true) {
				try {
					//Consumes records from the assigned partitions of 'TEQ' topic
					ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
					//Print consumed records
					for (ConsumerRecord<String, String> record : records)	
					{
						System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n ", record.partition(), record.offset(), record.key(), record.value());
						for(Header h: record.headers())
						{
							System.out.println("Header: " +h.toString());
						} 
					}
					//Commit all the consumed records
					if(records != null && records.count() > 0) {
						msgCnt += records.count();
						System.out.println("Committing records " + records.count());
						try {
							consumer.commitSync();
						}catch(Exception e)
						{
							System.out.println("Exception in commit " + e.getMessage());
							continue;
						}
						if(msgCnt >= expectedMsgCnt )
						{
							System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
							break;
						}
					}
					else {
						System.out.println("No Record Fetched. Retrying in 1 second");
						Thread.sleep(1000);
					}

				}catch(Exception e)
				{
					System.out.println("Inner Exception " + e.getMessage());
					throw e;
				}			
			}
		}catch(Exception e)
		{
			System.out.println("Exception from OKafka consumer " + e);
			e.printStackTrace();
		}finally {
			long runDuration = Duration.between(startTime, Instant.now()).toMillis();
			System.out.println("Closing OKafka Consumer. Received "+ msgCnt +" records. Run Duration " + runDuration);
			consumer.close();
		}
	}
}
 
