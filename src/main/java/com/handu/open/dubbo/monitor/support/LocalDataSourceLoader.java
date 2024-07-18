/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.handu.open.dubbo.monitor.support;

import com.google.common.base.Splitter;
import com.handu.open.dubbo.monitor.config.MyBatisConfig;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * for execute schema sql file.
 */
@Component
public class LocalDataSourceLoader implements EnvironmentAware {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDataSourceLoader.class);

    private static final String PRE_FIX = "file:";

    private static final String H2_DB_URL = "jdbc:h2";

    @Override
    public void setEnvironment(Environment bean) {
        String dbUrl = ((Environment) bean).getProperty(MyBatisConfig.DB_URL);
        if (StringUtils.isEmpty(dbUrl) || !dbUrl.startsWith(H2_DB_URL)) {
            return;
        }
        String username = ((Environment) bean).getProperty(MyBatisConfig.DB_USERNAME);
        String password = ((Environment) bean).getProperty(MyBatisConfig.DB_PASSWORD);
        String initScript = ((Environment) bean).getProperty(MyBatisConfig.DB_INIT_SCRIPT);
        this.init(dbUrl, username, password, initScript);
    }

    protected void init(String url, String username, String password, String initScript) {
        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            this.execute(connection, initScript);
        } catch (Exception e) {
            LOG.error("Datasource init error.", e);
            throw new RuntimeException(e);
        }
    }

    protected void execute(final Connection conn, final String script) throws Exception {
        ScriptRunner runner = new ScriptRunner(conn);
        try {
            // doesn't print logger
            runner.setLogWriter(null);
            runner.setAutoCommit(true);
            runner.setFullLineDelimiter(false);
            runner.setSendFullScript(false);
            runner.setStopOnError(false);
            Resources.setCharset(StandardCharsets.UTF_8);
            List<String> initScripts = Splitter.on(";").splitToList(script);
            for (String sqlScript : initScripts) {
                if (sqlScript.startsWith(PRE_FIX)) {
                    String sqlFile = sqlScript.substring(PRE_FIX.length());
                    try (Reader fileReader = getResourceAsReader(sqlFile)) {
                        LOG.info("execute dubbo monitor schema sql: {}", sqlFile);
                        runner.runScript(fileReader);
                    }
                } else {
                    try (Reader fileReader = Resources.getResourceAsReader(sqlScript)) {
                        LOG.info("execute dubbo monitor schema sql: {}", sqlScript);
                        runner.runScript(fileReader);
                    }
                }
            }
        } finally {
            runner.closeConnection();
        }
    }

    private static Reader getResourceAsReader(final String resource) throws IOException {
        return new InputStreamReader(Files.newInputStream(Paths.get(resource)), StandardCharsets.UTF_8);
    }

}
