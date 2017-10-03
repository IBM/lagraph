/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ibm
/** Provides classes for dealing with complex numbers.  Also provides
 *  implicits for converting to and from `Int`.
 * 
 *  ==Overview==
 *  The main class to use is [[com.ibm.lagraph.LagContext]], as so
 *  {{{
 *  scala> val complex = LagContext(4,3)
 *  complex: com.ibm.lagraph.LagContext = 4 + 3i
 *  }}}
 * 
 *  If you include [[com.ibm.lagraph.LagSemiring]], you can 
 *  convert numbers more directly
 *  {{{
 *  scala> import com.ibm.lagraph.LagSemiring._
 *  scala> val complex = 4 + 3.i
 *  complex: com.ibm.lagraph.Complex = 4 + 3i
 *  }}} 
 */
package object lagraph {}