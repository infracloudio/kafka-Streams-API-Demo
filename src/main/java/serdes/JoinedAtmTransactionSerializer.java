package serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import myapps.ATMFraud.JoinedAtmTransactions;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JoinedAtmTransactionSerializer implements Serializer<JoinedAtmTransactions> {
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to do
  }

  @Override
  public byte[] serialize(String topic, JoinedAtmTransactions data) {
    byte[] bytes = new byte[0];

    try {
      System.out.println("Trying to serialize: " + data);
      bytes = mapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      System.out.println("Error writing value as bytes");
    }

    return bytes;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
