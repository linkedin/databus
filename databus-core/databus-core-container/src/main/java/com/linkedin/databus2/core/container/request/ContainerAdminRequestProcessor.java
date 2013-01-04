package com.linkedin.databus2.core.container.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.concurrent.ExecutorService;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus.core.DatabusComponentStatus;

/** Implements simple container health-check REST interface */
public class ContainerAdminRequestProcessor implements RequestProcessor
{
  private final ExecutorService _executorService;
  private final DatabusComponentStatus _status;
  private final String _normalizedPath;
  private final String _statusPath;

  public ContainerAdminRequestProcessor(ExecutorService executorService,
                                        DatabusComponentStatus status,
                                        String healthcheckPath)
  {
    _executorService = executorService;
    int slashIdx = healthcheckPath.indexOf('/');
    _normalizedPath = -1 == slashIdx ? "" :
         normalizePath(healthcheckPath.substring(slashIdx));
    _statusPath = 0 == _normalizedPath.length() ? "status" : _normalizedPath + "/status";
    _status = status;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
                                                               RequestProcessingException
  {
    String path = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String normPath = normalizePath(path);
    if (null == normPath)
    {
      if (0 == _normalizedPath.length()) returnPlainStatus(request);
      else throw new RequestProcessingException("expected admin sub-command");
    }
    else if (_normalizedPath.equals(normPath)) returnPlainStatus(request);
    else if (_statusPath.equals(normPath)) returnComponentStatus(request);
    else throw new RequestProcessingException("unknown admin sub-command:" + normPath);

    return null;
  }

  public static String extractCommandRoot(String path)
  {
    int slashIndex = path.indexOf('/');
    return -1 == slashIndex ? path : path.substring(0, slashIndex);
  }

  private static String normalizePath(String path)
  {
    if (null == path) return null;
    int startIdx = 0;
    for (; startIdx < path.length() && path.charAt(startIdx) == '/'; ++startIdx);
    int endIdx = path.length() - 1;
    for (; endIdx >=0 && path.charAt(endIdx) == '/'; --endIdx);
    return path.substring(startIdx, endIdx + 1);
  }

  private void returnComponentStatus(DatabusRequest request) throws IOException
  {
    Formatter fmt = new Formatter();
    fmt.format("{\"status\":\"%s\",\"message\":\"%s\"}\n", _status.getStatus().toString(),
               _status.getMessage());
    fmt.flush();
    request.getResponseContent().write(ByteBuffer.wrap(fmt.toString().getBytes()));
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  private void returnPlainStatus(DatabusRequest request) throws IOException
  {
    String statusString;
    HttpResponseStatus statusCode;
    if (null != _status && _status.getStatus() != DatabusComponentStatus.Status.RUNNING)
    {
      statusString = "BAD\n";
      statusCode = HttpResponseStatus.SERVICE_UNAVAILABLE;
    }
    else
    {
      statusString = "GOOD\n";
      statusCode = HttpResponseStatus.OK;
    }

    request.getResponseContent().setResponseCode(statusCode);
    request.getResponseContent().write(ByteBuffer.wrap(statusString.getBytes()));
  }

}
