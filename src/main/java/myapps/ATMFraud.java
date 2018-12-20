package myapps;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import serdes.ATMTransactionTimestampExtractor;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.lang.Math.*;
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
    properties.put(BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, AtmTransactionSerde().getClass());

    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, AtmTransaction> atm_txns_gess = streamsBuilder
            .stream("my_atm_txns_gess", with(String(), AtmTransactionSerde(), new ATMTransactionTimestampExtractor(), EARLIEST))
            .filter(((key, value) -> value != null));

    final KStream<String, AtmTransaction> stream1 = atm_txns_gess
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

    final Topology leftTopology = streamsBuilder.build();
    System.out.println(leftTopology.describe());

    final KafkaStreams leftStreams = new KafkaStreams(leftTopology, properties);
    final CountDownLatch latch = new CountDownLatch(1);

    getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        leftStreams.close();
        latch.countDown();
      }
    });

    try {
      leftStreams.start();
      latch.await();
    } catch (Throwable t) {
      System.exit(1);
    }

    System.exit(0);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class JoinedAtmTransactions {
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss Z")
    @JsonProperty("prev_timestamp")
    private Date prevTimestamp;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss Z")
    @JsonProperty("later_timestamp")
    private Date laterTimestamp;
    @JsonProperty("distance_between_trxns_KM")
    private double distanceBetweenTrxnsKM;
    @JsonProperty("millisecond_difference")
    private long millisecondDifference;
    @JsonProperty("minutes_difference")
    private double minutesDifference;
    @JsonProperty("KMH_required")
    private double KMHRequired;
    @JsonProperty("account_id")
    private String accountId;
    @JsonProperty("prev_transaction_id")
    private UUID prevTransactionId;
    @JsonProperty("later_transaction_id")
    private UUID laterTransactionId;
    @JsonProperty("prev_transaction_location")
    private Location prevTransactionLocation;
    @JsonProperty("later_transaction_location")
    private Location laterTransactionLocation;

    public JoinedAtmTransactions(final AtmTransaction prevTrxn, final AtmTransaction laterTrxn) {
      prevTimestamp = prevTrxn.getTimestamp();
      laterTimestamp = laterTrxn.getTimestamp();
      distanceBetweenTrxnsKM = calculateDistanceInKM(prevTrxn.location.lat, prevTrxn.location.lon, laterTrxn.location.lat, laterTrxn.location.lon);
      millisecondDifference = laterTrxn.getTimestamp().getTime() - prevTrxn.getTimestamp().getTime();
      minutesDifference = (double) millisecondDifference / (1000 * 60);
      KMHRequired = distanceBetweenTrxnsKM / (minutesDifference/60);
      accountId = prevTrxn.getAccountId();
      prevTransactionId = prevTrxn.getTransactionId();
      laterTransactionId = laterTrxn.getTransactionId();
      prevTransactionLocation = prevTrxn.getLocation();
      laterTransactionLocation = laterTrxn.getLocation();
    }

    private double calculateDistanceInKM(double lat1, double lon1, double lat2, double lon2) {
      double d2r = (PI/180);
      double distance = 0;

      try {
        final double dlat = (lat2 - lat1) * d2r;
        final double dlong = (lon2 - lon1) * d2r;
        final double a = pow(sin(dlat / 2), 2) + cos(lat1 * d2r) * cos(lat2 * d2r) * pow(sin(dlong / 2.0), 2);
        final double c = 2 * atan2(sqrt(a), sqrt(1 - a));
        distance = 6367 * c;
      } catch (Exception e) {
        e.printStackTrace();
      }

      return distance;
    }

  public Date getPrevTimestamp() {
    return prevTimestamp;
  }

  public void setPrevTimestamp(Date prevTimestamp) {
    this.prevTimestamp = prevTimestamp;
  }

  public Date getLaterTimestamp() {
    return laterTimestamp;
  }

  public void setLaterTimestamp(Date laterTimestamp) {
    this.laterTimestamp = laterTimestamp;
  }

  public double getDistanceBetweenTrxnsKM() {
    return distanceBetweenTrxnsKM;
  }

  public void setDistanceBetweenTrxnsKM(double distanceBetweenTrxnsKM) {
    this.distanceBetweenTrxnsKM = distanceBetweenTrxnsKM;
  }

  public long getMillisecondDifference() {
    return millisecondDifference;
  }

  public void setMillisecondDifference(long millisecondDifference) {
    this.millisecondDifference = millisecondDifference;
  }

  public double getMinutesDifference() {
    return minutesDifference;
  }

  public void setMinutesDifference(double minutesDifference) {
    this.minutesDifference = minutesDifference;
  }

  public double getKMHRequired() {
    return KMHRequired;
  }

  public void setKMHRequired(double KMHRequired) {
    this.KMHRequired = KMHRequired;
  }

  public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public UUID getPrevTransactionId() {
    return prevTransactionId;
  }

  public void setPrevTransactionId(UUID prevTransactionId) {
    this.prevTransactionId = prevTransactionId;
  }

  public UUID getLaterTransactionId() {
    return laterTransactionId;
  }

  public void setLaterTransactionId(UUID laterTransactionId) {
    this.laterTransactionId = laterTransactionId;
  }

  public Location getPrevTransactionLocation() {
    return prevTransactionLocation;
  }

  public void setPrevTransactionLocation(Location prevTransactionLocation) {
    this.prevTransactionLocation = prevTransactionLocation;
  }

  public Location getLaterTransactionLocation() {
    return laterTransactionLocation;
  }

  public void setLaterTransactionLocation(Location laterTransactionLocation) {
    this.laterTransactionLocation = laterTransactionLocation;
  }

  @Override
  public String toString() {
    return "JoinedAtmTransactions{" +
            "prevTimestamp=" + prevTimestamp +
            ", laterTimestamp=" + laterTimestamp +
            ", distanceBetweenTrxnsKM=" + distanceBetweenTrxnsKM +
            ", millisecondDifference=" + millisecondDifference +
            ", minutesDifference=" + minutesDifference +
            ", KMHRequired=" + KMHRequired +
            ", accountId='" + accountId + '\'' +
            ", prevTransactionId=" + prevTransactionId +
            ", laterTransactionId=" + laterTransactionId +
            ", prevTransactionLocation=" + prevTransactionLocation +
            ", laterTransactionLocation=" + laterTransactionLocation +
            '}';
  }
}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AtmTransaction {
    @JsonProperty("account_id")
    private String accountId;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss Z")
    private Date timestamp;
    private String atm;
    private BigDecimal amount;
    private Location location;
    @JsonProperty("transaction_id")
    private UUID transactionId;

  public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public String getAtm() {
    return atm;
  }

  public void setAtm(String atm) {
    this.atm = atm;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public Location getLocation() {
    return location;
  }

  public void setLocation(Location location) {
    this.location = location;
  }

  public UUID getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(UUID transactionId) {
    this.transactionId = transactionId;
  }

  @Override
  public String toString() {
    return "AtmTransaction{" +
            "accountId='" + accountId + '\'' +
            ", timestamp=" + timestamp +
            ", atm='" + atm + '\'' +
            ", amount=" + amount +
            ", location=" + location +
            ", transactionId=" + transactionId +
            '}';
  }
}

  public static class Location{
    private double lat;
    private double lon;

    @Override
    public String toString() {
      return "(" + lat + ", " + lon + ")";
    }

    public double getLat() {
      return lat;
    }

    public void setLat(double lat) {
      this.lat = lat;
    }

    public double getLon() {
      return lon;
    }

    public void setLon(double lon) {
      this.lon = lon;
    }
  }
}
