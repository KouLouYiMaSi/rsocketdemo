package resume;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;

public class RequestCodec {

    public Payload encode(Request request) {
      String encoded = request.getChunkSize() + ":" + request.getFileName();
      return DefaultPayload.create(encoded);
    }

    public Request decode(Payload payload) {
      String encoded = payload.getDataUtf8();
      String[] chunkSizeAndFileName = encoded.split(":");
      int chunkSize = Integer.parseInt(chunkSizeAndFileName[0]);
      String fileName = chunkSizeAndFileName[1];
      return new Request(chunkSize, fileName);
    }
  }