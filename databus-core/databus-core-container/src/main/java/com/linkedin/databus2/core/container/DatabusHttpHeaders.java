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

/**
 * this class includes LITERAL for the HTTP headers and for the dbus protocol
 */
public class DatabusHttpHeaders
{
  public static final String DATABUS_HTTP_HEADER_PREFIX = "x-dbus-";

  // Headers that are present in a request
  public static final String DATABUS_REQUEST_ID_HEADER = DATABUS_HTTP_HEADER_PREFIX + "req-id";
  public static final String DATABUS_REQUEST_LATENCY_HEADER = DATABUS_HTTP_HEADER_PREFIX + "req-latency";

  // Headers that are present in a response
  public static final String DATABUS_ERROR_CLASS_HEADER = DATABUS_HTTP_HEADER_PREFIX + "error";
  public static final String DATABUS_ERROR_MESSAGE_HEADER = DATABUS_HTTP_HEADER_PREFIX + "error-message";
  public static final String DATABUS_ERROR_CAUSE_CLASS_HEADER = DATABUS_HTTP_HEADER_PREFIX + "error-cause";
  public static final String DATABUS_ERROR_CAUSE_MESSAGE_HEADER = DATABUS_HTTP_HEADER_PREFIX + "error-cause-message";
  public static final String DATABUS_PENDING_EVENT_SIZE = DATABUS_HTTP_HEADER_PREFIX + "pending-event-size";

  public static final String DBUS_SERVER_HOST_HDR = DATABUS_HTTP_HEADER_PREFIX + "server-host";
  public static final String DBUS_SERVER_SERVICE_HDR = DATABUS_HTTP_HEADER_PREFIX + "server-service";

  public static final String DBUS_CLIENT_HOST_HDR = DATABUS_HTTP_HEADER_PREFIX + "client-host";
  public static final String DBUS_CLIENT_SERVICE_HDR = DATABUS_HTTP_HEADER_PREFIX + "client-service";

  public static final String DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR = DATABUS_HTTP_HEADER_PREFIX + "protocol-version";

  /* databus2-relay's SourcesRequestProcessor has VERSION_PARAM_NAME = "v" that specifies the format
   * of the /sources response, but it's currently an unused capability; the client library doesn't
   * know about it. */

  /** protocol version param name for /register request */
  public static final String PROTOCOL_VERSION_PARAM = "protocolVersion";
  public static final String PROTOCOL_COMPRESS_PARAM = "compress";

  /** max event version - max DbusEvent version client can understand */
  public static final String MAX_EVENT_VERSION = "maxev";
}
