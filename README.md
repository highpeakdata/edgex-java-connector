# NexentaEdge Edge-X Java connector 
edgex-java-connector is Edge-X Java connector library

S3X interface for High-Performance load/unload/edit of billions of images
via object HTTP/S interface.

<p align="center">
  <img src="https://github.com/Nexenta/edgex-perl-connector/raw/master/edgefs-s3x-kv-benefits.png?raw=true" alt="edgefs-s3x-kv-benefits.png"/>
</p>

It is S3 compatible protocol, with extensions that allows batch operations
so that load of hundreds objects (like pictures, logs, packets, etc) can be
combined as one S3 emulated object.

The API documentation is available here https://nexenta.github.io/edgex-java-connector/

## Use cases details

* Advanced Versioned S3 Object Append and RW "Object as File" access
* S3 Object as a Key-Value database
* High-performance S3 Object Stream Session (RW)


## API examples

For API definitions see https://nexenta.github.io/edgex-java-connector/com/nexenta/edgex/EdgexClient.html

For Object examples see https://github.com/Nexenta/edgex-java-connector/blob/master/edgex-connector/src/test/java/com/nexenta/edgex/ObjectTest.java

For Key-Value examples https://github.com/Nexenta/edgex-java-connector/blob/master/edgex-connector/src/test/java/com/nexenta/edgex/KeyValueTest.java


## Quick start

On Ubuntu

1. Install Java jdk
   
   ```console
   apt-get install openjdk-8-jdk
   ```
   
2. Install maven
   
   ```console
   apt install maven
   ```   

3. Clone the edgex-java-connector repository

   ```console
   git clone https://github.com/Nexenta/edgex-java-connector.git
   cd edgex-java-connector/edgex-connector
   ```


4. Build edgex-java-connector

   ```console
   mvn install -Dmaven.test.skip=true
   ```
   
   The build process creates output jars:
   ```console
      target/edgex-connector-1.0-SNAPSHOT.jar,
      target/edgex-connector-1.0-SNAPSHOT-javadoc.jar
   ```
   and installes them to local maven repository.

5. Test edgex-java-connector with NexentaEdge cluster 

   ```console
   mvn test -Dedgex=http://<s3 gateway ip>:<port> -Dkey=<s3 key> -Dsecret=<s3 secret key>
   ```
   To test edgex-java-connector you need running NexentaEdge cluster and configured s3 Edge-X gateway service
   
   





