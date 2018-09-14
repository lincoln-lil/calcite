/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.schema;

import java.util.Map;

/**
 * Table who accept dynamic parameters.
 * Dynamic parameters can be used to specify IO behavior when manipulating a table.
 * eg:
 *   1. in a select clause, we can specify parameters such as
 *      start/stop conditions for a table representing message queue, fetch batch size etc.
 *   2. in a insert clause, we can specify parameters such as sink batch size.
 */
public interface ConfigurableTable extends Table {

  /**
   * Returns a new table with the dynamic parameters.
   **/
  Table config(Map<String, String> parameters);
}
// End ConfigurableTable.java
