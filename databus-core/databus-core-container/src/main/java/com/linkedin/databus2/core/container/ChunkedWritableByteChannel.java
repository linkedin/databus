/**
 * 
 */
package com.linkedin.databus2.core.container;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.nio.channels.WritableByteChannel;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Extends the {@link WritableByteChannel} interface for use in chunked HTTP responses. The regular
 * write operations create new chunks in the response. Two methods are added to support setting
 * of header values in the trailer (aka footers). Footers are serialized when the channel is closed.
 * @author cbotev
 *
 */
public interface ChunkedWritableByteChannel extends WritableByteChannel
{

  /**
   * Add a new header or footer value to the response. The value is appended to any existing values.
   * If no bytes from the response body have been sent, the name/value pair will be added as a
   * header. Otherwise, it will be added as a footer in the chunked encoding trailer.
   * @param name        the name of the footer
   * @param value       the value of the footer
   */
  public void addMetadata(String name, Object value);
  
  /**
   * Adds a new footer or replaces the value of an existing footer in the response. 
   * If no bytes from the response body have been sent, the name/value pair will be added as a
   * header. Otherwise, it will be added as a footer in the chunked encoding trailer.
   * @param name        the name of the footer
   * @param value       the value of the footer
   */
  public void setMetadata(String name, Object value);

  /**
   * Removes a footer from the response
   * If no bytes from the response body have been sent, the header with the specified name will be 
   * removed. Otherwise, the footer with that name will be removed. If there was a header with that
   * name and the first byte of the response body has been sent, the header value cannot be removed
   * anymore but no exception will be thrown.
   * 
   * @param name        the name of the footer
   */
  public void removeMetadata(String name);
  
  /**
   * Changes the HTTP response code. No change can be done after the first byte of the response has
   * been sent. Instead, a new footer x-databus-response-code will be added with the specified value. 
   * @param code
   */
  public void setResponseCode(HttpResponseStatus code);
  
  /**
   * Obtains the current HTTP response code.
   * @return the HTTP response code.
   */
  public HttpResponseStatus getResponseCode();
  
  /**
   * Get The Raw Channel
   */
  public Channel getRawChannel();
}
