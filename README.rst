=================================
Navigator Integration Application
=================================

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

Navigator Integration App is one such application built by the team at Cask for bridging CDAP Metadata
with Cloudera's data managemnet tool, Navigator. It's a CDAP native application that uses a real-time Flow to
fetch the CDAP Metadata and write it to Navigator.

- `Overview of CDAP Metadata <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/metadata-lineage.html#metadata>`__
- `Cloudera Navigator <http://www.cloudera.com/products/cloudera-navigator.html>`__
- `Real-time processing using Flows <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/flows-flowlets/index.html>`__
- `Kafka Flowlet Library <https://github.com/caskdata/cdap-packs/tree/develop/cdap-kafka-pack/cdap-kafka-flow>`__
- `Navigator SDK <https://github.com/cloudera/navigator-sdk>`__


Getting Started
===============

Prerequisites
-------------
To use Navigator Integration App, you need CDAP version 3.2.1 or higher and Navigator version of 2.4.0 or greater.

Metadata publishing to Kafka
----------------------------
Navigator Integration App contains a Flow that subscribes to the Kafka topic to which CDAP Metadata system publishes
the metadata updates. Hence, before using this application, the user should enable publishing of metadata updates to
Kafka.

- `Enable Metadata Update Notifications <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/metadata-lineage.html#metadata-update-notifications>`__

Building Plugins
----------------
You get started by building directly from the latest source code::

  git clone https://github.com/caskdata/cdap-navigator.git
  cd cdap-navigator
  mvn clean package

After the build completes, you will have a JAR under: ``target/`` directory.

Deploying Navigator Integration App
-----------------------------------

Step 1: Start by deploying the artifact JAR built using from source or download the released JAR from Maven.
Deploy the JAR using the CDAP CLI::

  > load artifact <target/navigator-<version>-jar>


Step 2: Create an application configuration file that contains:

- Kafka Metadata Config: Kafka Consumer Flowlet configuration information (info about where we can fetch metadata updates)
- Navigator Config: Information required by the Navigator Client to publish data to Navigator

Sample Application Configuration file::

{
	"config": {
		"metadataKafkaConfig": {
			"zookeeperString": "hostname:2181/cdap/kafka"
		},
		"navigatorConfig": {
			"navigatorHostName": "navigatormetadataserver",
			"username": "abcd",
			"password": "1234"
		}
	}
}

Metadata Kafka Config:

This key contains a property map that contains the following properties:

Required Properties:
- ``zookeeperString`` : Kafka Zookeeper string that can be used to subscribe to the CDAP metadata updates
- ``brokerString`` : Kafka Broker string to which CDAP metadata is published

Note: The user can specify either zookeeperString or brokerString.

Optional Properties:
- ``topic`` : Kafka Topic to which CDAP Metadata updates are published. Default is ``cdap-metadata-updates`` which
corresponds to the default topic used in CDAP for Metadata updates.
- ``numPartitions`` : Number of Kafka partitions. Default is set to ``10``.
- ``offsetDataset`` : Name of the dataset where Kafka offsets are stored. Default is ``kafkaOffset``.

Navigator Config:

This key contains a property map that contains the following properties:

Required Properties:
- ``navigatorHostName`` : Navigator Metadata Server hostname
- ``username`` : Navigator Metadata Server username
- ``password`` : Navigator Metadata Server password

Optional Properties:
- ``navigatorPort`` : Navigator Metadata Server port. Default is ``7187``.
- ``autocommit`` : Navigator SDK's autocommit property. Default is ``false``.
- ``namespace`` : Navigator namespace. Default is ``CDAP``.
- ``applicationURL`` : Navigator Application URL. Default is ``http://navigatorHostName``.
- ``fileFormat`` : Navigator File Format. Default is ``JSON``.
- ``navigatorURL`` : Navigator URL. Default is ``http://navigatorHostName:navigatorPort/api/v8``.
- ``metadataParentURI`` : Navigator Metadata Parent URI. Default is ``http://navigatorHostName:navigatorPort/api/v8/metadata/plugin``.

Step 3: Create a CDAP Application by providing a configuration file::

  > create app metaApp navigator 1.0.0 USER appconfig.txt

Step 4: Start the MetadataFlow::

  > start flow metaApp.MetadataFlow

You should now be able to view CDAP Metadata in Navigator UI. Note that all CDAP Entities use ``SDK`` as
the SourceType and uses ``CDAP`` as the namespace (this can be changed). Since Navigator SDK doesn't allow adding
new EntityTypes, we have used the following mapping:

CDAP EntityType  |  Navigator EntityType
----------------------------------------
Application          File
Artifact             File
Dataset              Dataset
Program              Operation
Stream               Dataset
StreamView           Table

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2016 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
