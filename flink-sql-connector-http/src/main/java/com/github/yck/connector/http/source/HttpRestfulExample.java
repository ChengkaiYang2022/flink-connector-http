/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.yck.connector.http.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class HttpRestfulExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Remember change hostname to your own hostname that send socket data.
        final String hostname = params.get("path", "/flink/table2");
        final String port = params.get("port", "8081");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)\n" +
                "WITH (\n" +
                "'connector' = 'http-restful',\n" +
                "'path' = '/flink/table1',\n" +
                "'port' = '8080',\n" +
                "'format' = 'http-restful-json'\n" +
                ");\n");

        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT name, score " +
                "FROM UserScores");

        // print the result to the console
        tEnv.toChangelogStream(result).print();

        env.execute();
    }
}
