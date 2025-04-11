package CDN.Anonymizer;

import CDN.HttpRecord.HttpLog;
import CDN.HttpRecord.HttpLogDTO;
import org.apache.kafka.clients.consumer.*;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListener.class);
    private final KafkaConsumer<String, byte[]> consumer;
    private final  DataSender dataSender;
    private List<HttpLogDTO> dtoList;
    private final long kafkaPullDelay = Long.parseLong(System.getenv("KAFKA-POLL-DELAY-MS"));
    private final long proxyCoolDown = Long.parseLong(System.getenv("PROXY-COOLDOWN"));
    private final long aggregationCoolDown = Long.parseLong(System.getenv("DATA-REFRESH-COOLDOWN"));

    public KafkaListener(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "capnp-listener-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        dtoList = new ArrayList<>();

        this.dataSender = new DataSender();
        dataSender.connectToProxy();
    }

    public void listen() {
        logger.info("Start listening");

        while (true) {
            try {
                Thread.sleep(kafkaPullDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Listener interrupted during sleep.");
                break;
            }
            ConsumerRecords<String, byte[]> records = consumer.poll(java.time.Duration.ofMillis(100));

            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(record.value());
                    MessageReader message = Serialize.read(buffer);
                    HttpLog.HttpLogRecord.Reader log = message.getRoot(HttpLog.HttpLogRecord.factory);
                    HttpLogDTO logDTO = new HttpLogDTO(log.getTimestampEpochMilli(),log.getResourceId(), log.getBytesSent(), log.getRequestTimeMilli(), log.getResponseStatus(), log.getCacheStatus().toString(), log.getMethod().toString(), log.getRemoteAddr().toString(), log.getUrl().toString());

                    if(logDTO.getRemoteAddr() != null && logDTO.getTimestampEpochMilli() > 0 && logDTO.getBytesSent() > 0 && logDTO.getResourceId() > 0 && logDTO.getRequestTimeMilli() > 0) {
                        dtoList.add(logDTO);
                    }else{
                        logger.error("Trying to add log record with variable overflow or/and invalid remote address. this record will be ignored");
                    }
                } catch (Exception e) {
                    logger.error("Failed to deserialize Cap’n Proto message: " + e.getMessage());
                }
            }

            try {
                consumer.commitSync();
                logger.debug("Committed offsets after adding logs to dtoList.");
            } catch (CommitFailedException e) {
                logger.error("Offset commit failed: " + e.getMessage());
            }

            if(dataSender.getProxyDuration() > proxyCoolDown && dataSender.getAggRefreshDuration() < aggregationCoolDown){
                 dtoList = dataSender.transferData(dtoList);
            }

            if(dataSender.getProxyDuration() > proxyCoolDown && dataSender.getAggRefreshDuration() > aggregationCoolDown){
                dataSender.refreshAggregatedTable();
            }
        }
    }
}