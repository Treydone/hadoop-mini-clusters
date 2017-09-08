/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KdcLocalClusterZookeeperIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterZookeeperIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static KdcLocalCluster kdcLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        System.setProperty("sun.security.krb5.debug", "true");

        // KDC
        kdcLocalCluster = new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
        kdcLocalCluster.start();

        System.setProperty("java.security.krb5.conf", kdcLocalCluster.getKrb5conf().getAbsolutePath());

        // Config is statically initialized at this point. But the above configuration results in a different
        // initialization which causes the tests to fail. So the following two changes are required.

        // (1) Refresh Kerberos config.
        // refresh the config
        Class<?> classRef;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        Method refreshMethod = classRef.getMethod("refresh", new Class[0]);
        refreshMethod.invoke(classRef, new Object[0]);
        // (2) Reset the default realm.
        Field defaultRealm = org.apache.zookeeper.server.auth.KerberosName.class.getDeclaredField("defaultRealm");
        defaultRealm.setAccessible(true);
        defaultRealm.set(null, org.apache.zookeeper.server.util.KerberosUtil.getDefaultRealm());
        System.err.println("ZOOKEEPER: Using default realm " + org.apache.zookeeper.server.util.KerberosUtil.getDefaultRealm());

        // Zookeeper
        javax.security.auth.login.Configuration.setConfiguration(new Jaas()
                .addServiceEntry("Server", kdcLocalCluster.getKrbPrincipal("zookeeper"), kdcLocalCluster.getKeytabForPrincipal("zookeeper"), "zookeeper"));

        Map<String, Object> properties = new HashMap<>();
        properties.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        properties.put("requireClientAuthScheme", "sasl");
        //properties.put("zookeeper.kerberos.removeHostFromPrincipal", "true");
        //properties.put("zookeeper.kerberos.removeRealmFromPrincipal", "true");
        properties.put("zookeeper.sasl.serverconfig", "Server");

        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setCustomProperties(properties)
                .build();
        zookeeperLocalCluster.start();

        System.setProperty("zookeeper.sasl.client", "true");
        System.setProperty("zookeeper.sasl.clientconfig", "Client");
        javax.security.auth.login.Configuration.setConfiguration(new Jaas()
                .addEntry("Client", "guest", kdcLocalCluster.getKeytabForPrincipal("guest")));

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperLocalCluster.getZookeeperConnectionString(),
                new ExponentialBackoffRetry(1000, 3))) {
            client.start();
            System.err.println("Content for /");
            client.getChildren().forPath("/").forEach(System.err::println);
            System.err.println("ACLs for /");
            client.getACL().forPath("/").forEach(System.err::println);

            List<ACL> perms = new ArrayList<>();
            perms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
            perms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

            client.create().withMode(CreateMode.PERSISTENT).withACL(perms).forPath(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY));
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        zookeeperLocalCluster.stop();
        kdcLocalCluster.stop();
    }

    @Test
    public void testZookeeper() throws Exception {

    }
}
