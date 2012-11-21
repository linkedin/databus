package com.linkedin.databus.client;


public interface DatabusStreamConnectionStateMessage
{
  void switchToStreamRequestError();
  void switchToStreamResponseError();
  void switchToStreamSuccess(ChunkedBodyReadableByteChannel result);
}
