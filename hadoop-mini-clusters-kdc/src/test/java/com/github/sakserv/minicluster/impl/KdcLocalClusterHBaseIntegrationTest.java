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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KdcLocalClusterHBaseIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterHBaseIntegrationTest.class);

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
    private static HbaseLocalCluster hbaseLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        // Zookeeper
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.start();

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
        Class<?> classRef;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        Method refreshMethod = classRef.getMethod("refresh", new Class[0]);
        refreshMethod.invoke(classRef, new Object[0]);
        // (2) Reset the default realm.
        Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
        defaultRealm.setAccessible(true);
        defaultRealm.set(null, KerberosUtil.getDefaultRealm());
        System.err.println("HADOOP: Using default realm " + KerberosUtil.getDefaultRealm());

        Configuration baseConf = kdcLocalCluster.getBaseConf();

        // HBase
        HBaseKerberosUtils.setKeytabFileForTesting(kdcLocalCluster.getKeytabForPrincipal("hbase"));
        HBaseKerberosUtils.setPrincipalForTesting(kdcLocalCluster.getKrbPrincipalWithRealm("hbase"));
        assertTrue(HBaseKerberosUtils.isKerberosPropertySetted());

        javax.security.auth.login.Configuration.setConfiguration(new Jaas()
                        .addEntry("HBaseServer", kdcLocalCluster.getKrbPrincipalWithRealm("hbase"), kdcLocalCluster.getKeytabForPrincipal("hbase"))
                //.addEntry("Client", "zookeeper", kdcLocalCluster.getKeytabForPrincipal("guest"))
        );

        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.addResource(baseConf);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(hbaseConfig)
                .build();
        hbaseLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseLocalCluster.stop();
        zookeeperLocalCluster.stop();
        kdcLocalCluster.stop();
    }

    @Test
    public void testHBase() throws Exception {
        UserGroupInformation.loginUserFromKeytab(kdcLocalCluster.getKrbPrincipalWithRealm("hbase"), kdcLocalCluster.getKeytabForPrincipal("hbase"));

        assertTrue(UserGroupInformation.isSecurityEnabled());
        assertTrue(UserGroupInformation.isLoginKeytabBased());

        Configuration configuration = hbaseLocalCluster.getHbaseConfiguration();

        // Write data
        final HBaseAdmin admin = new HBaseAdmin(configuration);

        TableName tableName = TableName.valueOf("test-kdc");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        // Log out
        UserGroupInformation.getLoginUser().logoutUserFromKeytab();

        UserGroupInformation.reset();

        try {
            Configuration conf = new Configuration();
            UserGroupInformation.setConfiguration(conf);
            // TODO
            System.err.println(UserGroupInformation.getLoginUser());
            fail();
        } catch (AccessControlException e) {
            System.out.println("Not authenticated!");
        }
    }
}
