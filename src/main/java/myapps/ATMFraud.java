package myapps;

import model.AtmTransaction;
import model.JoinedAtmTransactions;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import serdes.ATMTransactionTimestampExtractor;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.Runtime.getRuntime;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.kstream.Consumed.with;
import static serdes.ATMSerdes.AtmTransactionSerde;
import static serdes.ATMSerdes.JoinedAtmTransactionsSerde;

/**
 * {
 * "account_id": "a54",
 * "timestamp": "2018-12-13 11:23:07 +0000",
 * "atm": "ATM : 301736434",
 * "amount": 50,
 * "location": {"lat": "53.7916054", "lon": "-1.7471223"},
 * "transaction_id": "77620dac-fec9-11e8-9027-0242ac1c0007"
 * }
 */
public class ATMFraud {
  public static void main(final String[] args) {
    Properties properties = new Properties();
    properties.put(APPLICATION_ID_CONFIG, "streams-atm-fraud-detector");
    properties.put(BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, AtmTransactionSerde().getClass());

    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, AtmTransaction> stream1 = streamsBuilder
            .stream("my_atm_txns_gess", with(String(), AtmTransactionSerde(), new ATMTransactionTimestampExtractor(), EARLIEST))
            .filter(((key, value) -> value != null))
            .map((key, value) -> KeyValue.pair(value.getAccountId(), value));

    final KStream<String, AtmTransaction> stream2 = stream1.map((KeyValue::pair));

    final KStream<String, JoinedAtmTransactions> joinedStream = stream1.join(stream2,
            JoinedAtmTransactions::new,
            JoinWindows.of(Duration.ofMinutes(10)).before(Duration.ZERO));

    final KStream<String, JoinedAtmTransactions> filteredStream = joinedStream.filter(((accountId, joinedTrxn) ->
    {
      boolean result = false;
      if (joinedTrxn.getPrevTransactionId().equals(joinedTrxn.getLaterTransactionId()))
        System.out.println("transaction IDS match. record will be skipped. Prev Trxn Id: " + joinedTrxn.getPrevTransactionId()
        + "Later Trxn Id: " + joinedTrxn.getLaterTransactionId());
      else if (joinedTrxn.getPrevTimestamp().equals(joinedTrxn.getLaterTimestamp()))
        System.out.println("transaction TIMES match. record will be skipped. Prev Trxn Id: " + joinedTrxn.getPrevTransactionId()
                + "Later Trxn Id: " + joinedTrxn.getLaterTransactionId());
      else if (joinedTrxn.getPrevTransactionLocation().toString().equals(joinedTrxn.getLaterTransactionLocation().toString()))
        System.out.println("transaction LOCATIONS match. record will be skipped. Prev Trxn Id: " + joinedTrxn.getPrevTransactionId()
                + "Later Trxn Id: " + joinedTrxn.getLaterTransactionId());
      else {
        System.out.println("FRAUDULOUS transaction found. Prev Trxn Id: " + joinedTrxn.getPrevTransactionId()
                + "Later Trxn Id: " + joinedTrxn.getLaterTransactionId());
        result = true;
      }

      return result;
    }));

    final KStream<String, JoinedAtmTransactions> printFraudTrxnsStream = filteredStream.mapValues(value -> {
      System.out.println("--------------------------------------------------------------------------------------------");
      System.out.println("Fraudulent transaction: " + value);
      return value;
    });

    printFraudTrxnsStream.to("my_atm_txns_fraudulent", Produced.with(String(), JoinedAtmTransactionsSerde()));

    final Topology topology = streamsBuilder.build();
    System.out.println(topology.describe());

    final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
    final CountDownLatch latch = new CountDownLatch(1);

    getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        kafkaStreams.close();
        latch.countDown();
      }
    });

    try {
      kafkaStreams.start();
      latch.await();
    } catch (Throwable t) {
      System.exit(1);
    }

    System.exit(0);
  }
}
