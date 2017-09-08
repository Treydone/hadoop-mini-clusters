package com.github.sakserv.minicluster.impl;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

public class Jaas extends Configuration {

    private static final String krb5LoginModuleName;

    static {
        if (System.getProperty("java.vendor").contains("IBM")) {
            krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
        } else {
            krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        }
    }

    private Map<String, AppConfigurationEntry> entries = new HashMap();

    public Jaas addServiceEntry(String name, String principal, String keytab, String serviceName) {
        Map<String, String> options = common(principal, keytab);
        options.put("serviceName", serviceName);
        entries.put(name, new AppConfigurationEntry(krb5LoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options));
        return this;
    }

    public Jaas addEntry(String name, String principal, String keytab) {
        Map<String, String> options = common(principal, keytab);
        entries.put(name, new AppConfigurationEntry(krb5LoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options));
        return this;
    }

    protected static Map<String, String> common(String principal, String keytab) {
        Map<String, String> options = new HashMap<>();
        options.put("keyTab", keytab);
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("useTicketCache", "false");
        options.put("debug", "true");
        return options;
    }

    public void removeEntry(String name) {
        entries.remove(name);
    }

    public void clear() {
        entries.clear();
    }

    public Map<String, AppConfigurationEntry> getEntries() {
        return entries;
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[]{entries.get(name)};
    }
}
