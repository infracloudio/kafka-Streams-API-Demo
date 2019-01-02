package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.AtmTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.io.IOException;

public class ATMTransactionTimestampExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
    final Object value = record.value();

    if (value instanceof byte[]) {
      return extractTimestampFromStringValue(new String((byte[]) value));
    }
    else if (value instanceof String) {
      return extractTimestampFromStringValue((String) value);
    } else if (value instanceof AtmTransaction) {
      return ((AtmTransaction) value).getTimestamp().getTime();
    } else if (value == null) {
      System.out.println("value is null: " + value);
      return 0;
    } else {
      System.out.println("Received value of type" + value.getClass().getName() + ". No matching time extractor present.");
    }

    return 0;
  }

  private long extractTimestampFromStringValue(String value) {
    try {
      final AtmTransaction atmTransaction = new ObjectMapper().readValue(value, AtmTransaction.class);
      return atmTransaction.getTimestamp().getTime();
    } catch (IOException e) {
      System.out.println("Error while converting string to AtmTransaction");
      e.printStackTrace();
    }

    return 0;
  }
}
