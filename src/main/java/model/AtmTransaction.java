package model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AtmTransaction {
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
