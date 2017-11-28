# Hadoop (with Kerberos) UGI Usage Tests
This project was created to test hadoop-client's UserGroupInformation class under various use cases that differ from the standard scenario of a single service principal proxying user principals.  These tests have been performed using version 2.7.3 and 2.8.2 of hadoop-client.
#### [UGI Relogin with Unintentional Impersonation](#ugi-relogin-with-unintentional-impersonation-1)  
#### [TGT Expiration](#tgt-expiration-1)
#### Building from source
``mvn clean install``
#### Command line parameters and usage example
```
-c,--task-config <file>                  Task configuration file (JSON)
-h,--help                                display help/usage info
-k,--krb-conf <file>                     Kerberos configuration path
-m,--hadoop-relogin-min-delay <seconds>  Hadoop minimum relogin period (in seconds), only supported with hadoop-client 2.8+
-r,--hadoop-resource <file>              site.xml file used by Hadoop (repeatable argument)
```
Example: ``java -jar ugi-test-<version>-jar-with-dependencies.jar
    -k krb5.conf
    -c tasks-config.json
    -r core-site.xml
    -r hdfs-site.xml``
# UGI Relogin with Unintentional Impersonation
#### Description of the test
Multiple principals that are logged in using UGI instances that are instantiated from a UGI class loaded by the same classloader will encounter problems when the second principal attempts to relogin.  An impersonation will occur and the operation attempted by the second principal after relogging in will fail.
#### Setup for the test
1. Create two principals for the test
    * ``kadmin.local -q 'addprinc -randkey -maxlife "10 seconds" -maxrenewlife "10 minutes" ugitest1@EXAMPLE.COM'``
    * ``kadmin.local -q 'addprinc -randkey -maxlife "10 seconds" -maxrenewlife "10 minutes" ugitest2@EXAMPLE.COM'``
2. Export the principals to keytabs
    * ``kadmin.local -q 'ktadd -k ugitest1.keytab ugitest1@EXAMPLE.COM'``
    * ``kadmin.local -q 'ktadd -k ugitest2.keytab ugitest2@EXAMPLE.COM'``
3. Copy the keytabs to a place where the test will be able to access them
4. Make sure krb5.conf exists for the KDC used to create the principals and that it is accessible by the test
5. Make sure core-site.xml and hdfs-site.xml files are accessible by the test.
6. Create a task config with the following json:
```json
[{
  "keytabPath": "/path/to/ugitest1.keytab",
  "principal": "ugitest1@EXAMPLE.COM",
  "reloginPeriod": 11,
  "taskPeriod": 11,
  "initialTaskDelay": 60,
  "destinationPath": "/writable/hdfs/path"
},
{
  "keytabPath": "/path/to/ugitest2.keytab",
  "principal": "ugitest2@EXAMPLE.COM",
  "reloginPeriod": 11,
  "taskPeriod": 11,
  "initialTaskDelay": 60,
  "destinationPath": "/writable/hdfs/path"
}]
```
# TGT Expiration
If the keytab used to create an initial valid UGI instance is available to hadoop-client, UGI#reloginFromKeytab (which is called by UGI#checkTGTAndReloginFromKeytab) should be able to acquire a new TGT if the existing one has expired, but does not, given the following setup.  Please be aware that the maxlife (__10 minutes__) and task config settings are different from the [UGI Relogin with Unintentional Impersonation](#ugi-relogin-with-unintentional-impersonation-1) test and create an edge-case scenario to recreate the issue.
##### Setup for the test
1. Create a principal for the test
    * ``kadmin.local -q 'addprinc -randkey -maxlife "10 minutes" -maxrenewlife "10 minutes" ugitest3@EXAMPLE.COM'``
2. Export the principal to a keytab
    * ``kadmin.local -q 'ktadd -k ugitest3.keytab ugitest3@EXAMPLE.COM'``
3. Copy the keytab to a place where the test will be able to access it
4. Make sure krb5.conf exists for the KDC used to create the principal and that it is accessible by the test
5. Make sure core-site.xml and hdfs-site.xml files are accessible by the test.
6. Create a task config with the following json:
```json
[{
  "keytabPath": "/path/to/ugitest3.keytab",
  "principal": "ugitest3@HDF.COM",
  "reloginPeriod": 241,
  "taskPeriod": 120,
  "initialTaskDelay": 0,
  "destinationPath": "/writable/hdfs/path"
}]
```
