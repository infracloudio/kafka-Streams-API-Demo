package model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.UUID;

import static java.lang.Math.*;
import static java.lang.Math.atan2;
import static java.lang.Math.sqrt;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinedAtmTransactions {
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
    distanceBetweenTrxnsKM = calculateDistanceInKM(prevTrxn.getLocation().getLat(),
            prevTrxn.getLocation().getLon(),
            laterTrxn.getLocation().getLat(),
            laterTrxn.getLocation().getLon());
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
