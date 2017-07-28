# Hadoop UGI relogin test using multiple principals

## Description of the test
Multiple principals that are logged in using UGI instances that are instantiated from a UGI class loaded by the same
classloader will encounter problems when the second principal attempts to relogin.  An impersonation will occur and
the operation attempted by the second principal after relogging in will fail.

## Setup for the test
1. Create two principals for the test
    * kadmin.local -q 'addprinc -randkey -maxlife "10 seconds" -maxrenewlife "10 minutes" ugitest1@EXAMPLE.COM' 
    * kadmin.local -q 'addprinc -randkey -maxlife "10 seconds" -maxrenewlife "10 minutes" ugitest2@EXAMPLE.COM'
2. Export the principals to keytabs
    * kadmin.local -q 'ktadd -k ugitest1.keytab ugitest1@EXAMPLE.COM'
    * kadmin.local -q 'ktadd -k ugitest2.keytab ugitest2@EXAMPLE.COM'
3. Copy the keytabs to a place where the test will be able to access them
4. Make sure krb5.conf exists for the KDC used to create the principals and that it is accessible by the test
5. Make sure core-site.xml and hdfs-site.xml files are accessible by the test.

## Building the code
mvn clean install

## Running the test
java -jar ugi-test-1.0-SNAPSHOT-jar-with-dependencies.jar 
    -k krb5.conf 
    -c ugitest1.keytab,ugitest1@EXAMPLE.COM 
    -c ugitest2.keytab,ugitest2@EXAMPLE.COM 
    -r core-site.xml,hdfs-site.xml 
    -p 11
    -d /existing/hdfs/path