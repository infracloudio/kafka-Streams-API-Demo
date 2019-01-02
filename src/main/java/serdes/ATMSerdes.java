package serdes;

import model.AtmTransaction;
import model.JoinedAtmTransactions;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

public class ATMSerdes extends WrapperSerde<AtmTransaction> {
  public ATMSerdes(Serializer<AtmTransaction> serializer, Deserializer<AtmTransaction> deserializer) {
    super(serializer, deserializer);
  }

  static public Serde<AtmTransaction> AtmTransactionSerde() {
    return new AtmTransactionSerde();
  }

  static public Serde<JoinedAtmTransactions> JoinedAtmTransactionsSerde() {
    return new JoinedAtmTransactionSerde();
  }

  static public final class AtmTransactionSerde extends WrapperSerde<AtmTransaction> {
    public AtmTransactionSerde() {
      super(new AtmTransactionSerializer(), new AtmTransactionDeserializer());
    }
  }

  static public final class JoinedAtmTransactionSerde extends WrapperSerde<JoinedAtmTransactions> {
    public JoinedAtmTransactionSerde() {
      super(new JoinedAtmTransactionSerializer(), new JoinedAtmTransactionDeserializer());
    }
  }
}
