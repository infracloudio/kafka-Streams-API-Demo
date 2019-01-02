package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.AtmTransaction;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class AtmTransactionDeserializer implements Deserializer<AtmTransaction> {
  private ObjectMapper mapper = new ObjectMapper();
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    //nothing to do.
  }

  @Override
  public AtmTransaction deserialize(String topic, byte[] data) {
    AtmTransaction atmTransaction = null;
    final String transactionString = new String(data);
    try {
      atmTransaction = mapper.readValue(transactionString, AtmTransaction.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return atmTransaction;
  }

  @Override
  public void close() {
    //nothing to do.
  }
}
