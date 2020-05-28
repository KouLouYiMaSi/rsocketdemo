package resume;

public class Request {
    private final int chunkSize;
    private final String fileName;

    public Request(int chunkSize, String fileName) {
      this.chunkSize = chunkSize;
      this.fileName = fileName;
    }

    public int getChunkSize() {
      return chunkSize;
    }

    public String getFileName() {
      return fileName;
    }
  }