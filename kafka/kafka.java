import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.common.kafkaFuture;
import org.apache.okafka.clients.admin.AdminClient;

public class AdminKafka {

    public static void main(String[] args){
        Properties props = new Properties;
        //donde se corre oracle y el puerto 
        props.put("bootsrap.servers", "localhost:5000");

        //nombre de los servicios que se ejecutan en la instancia de base de datos
        props.put("oracle.service.name", "jwt");
        props.put("security.protocol", "PLAINTEXT");
    }
}

