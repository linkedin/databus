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
package com.linkedin.databus.core.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link StringUtils}
 */
public class TestStringUtils
{

 @Test
 public void testSanitizeDbUri()
 {
   String s = StringUtils.sanitizeDbUri("jdbc:oracle:thin:uSeR/p4ssw0rd@db.company.com:1521/SCHEMA");
   Assert.assertEquals(s, "jdbc:oracle:thin:*/*@db.company.com:1521/SCHEMA");

   s = StringUtils.sanitizeDbUri("jdbc:oracle:thick:uSeR/p4ssw0rd@db.company.com:1521/SCHEMA");
   Assert.assertEquals(s, "jdbc:oracle:thick:uSeR/p4ssw0rd@db.company.com:1521/SCHEMA");

   s = StringUtils.sanitizeDbUri("jdbc:oracle:thin:fancy-uri/v1/v2(123,456,789)@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS=(PROTOCOL=TCP)(HOST=db1.host.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=db2.host.com)(PORT=1521))(CONNECT_DATA=(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))");
   Assert.assertEquals(s, "jdbc:oracle:thin:*/*@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS=(PROTOCOL=TCP)(HOST=db1.host.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=db2.host.com)(PORT=1521))(CONNECT_DATA=(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))");

   s = StringUtils.sanitizeDbUri("jdbc:mysql://address=(protocol=tcp)(host=localhost)(port=3306)(password=p4ssw0rd)(user=uSeR)/db");
   Assert.assertEquals(s, "jdbc:mysql://address=(protocol=tcp)(host=localhost)(port=3306)(password=*)(user=*)/db");

   s = StringUtils.sanitizeDbUri("jdbc:mysql://localhost:3306/DB?profileSQL=true&user=Godzilla&password=KingKong");
   Assert.assertEquals(s, "jdbc:mysql://localhost:3306/DB?profileSQL=true&user=*&password=*");

   s = StringUtils.sanitizeDbUri("jdbc:MySqL://localhost:3306/DB?profileSQL=true&user=Godzilla&password=KingKong");
   Assert.assertEquals(s, "jdbc:MySqL://localhost:3306/DB?profileSQL=true&user=Godzilla&password=KingKong");
 }

}
