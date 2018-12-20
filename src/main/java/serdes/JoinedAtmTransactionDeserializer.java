package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import myapps.ATMFraud.JoinedAtmTransactions;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JoinedAtmTransactionDeserializer implements Deserializer<JoinedAtmTransactions> {
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public JoinedAtmTransactions deserialize(String topic, byte[] data) {
    JoinedAtmTransactions value = null;

    try {
//      System.out.println("Trying to deserialize data: " + new String(data));
      value = mapper.readValue(data, JoinedAtmTransactions.class);
    } catch (IOException e) {
      System.out.println("Error while deserializing data.");
    }

    return value;
  }

  @Override
  public void close() {

  }
}
