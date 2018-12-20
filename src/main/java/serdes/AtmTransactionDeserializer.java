package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import myapps.ATMFraud;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class AtmTransactionDeserializer implements Deserializer<ATMFraud.AtmTransaction> {
  private ObjectMapper mapper = new ObjectMapper();
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    //nothing to do.
  }

  @Override
  public ATMFraud.AtmTransaction deserialize(String topic, byte[] data) {
    ATMFraud.AtmTransaction atmTransaction = null;
    final String transactionString = new String(data);
    try {
//      System.out.println("Incoming String : " + transactionString);
      atmTransaction = mapper.readValue(transactionString, ATMFraud.AtmTransaction.class);
    } catch (IOException e) {
//      System.out.println("Deserialization failed.");
      e.printStackTrace();
    }
    return atmTransaction;
  }

  @Override
  public void close() {
    //nothing to do.
  }
}
