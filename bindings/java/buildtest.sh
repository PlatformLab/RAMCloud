#!/bin/bash

cd ../..

make clean
make -j12 test
g++ -shared -fPIC -o bindings/java/lib/libramcloudtest.so \
    obj.javabindings/AbstractLog.o obj.javabindings/BackupClient.o \
    obj.javabindings/BackupFailureMonitor.o \
    obj.javabindings/BackupMasterRecovery.o obj.javabindings/BackupSelector.o \
    obj.javabindings/BackupService.o obj.javabindings/BackupStorage.o \
    obj.javabindings/CleanableSegmentManager.o \
    obj.javabindings/CoordinatorServerList.o \
    obj.javabindings/CoordinatorService.o \
    obj.javabindings/CoordinatorUpdateInfo.pb.o \
    obj.javabindings/CoordinatorUpdateManager.o \
    obj.javabindings/DataBlock.o obj.javabindings/Enumeration.o \
    obj.javabindings/EnumerationIterator.o \
    obj.javabindings/EnumerationIterator.pb.o \
    obj.javabindings/FailureDetector.o obj.javabindings/HashTable.o \
    obj.javabindings/InMemoryStorage.o obj.javabindings/IndexletManager.o \
    obj.javabindings/Log.o obj.javabindings/LogCleaner.o \
    obj.javabindings/LogDigest.o obj.javabindings/LogEntryRelocator.o \
    obj.javabindings/LogIterator.o obj.javabindings/MasterRecoveryManager.o \
    obj.javabindings/MasterService.o obj.javabindings/MasterTableMetadata.o \
    obj.javabindings/MembershipService.o \
    obj.javabindings/MinCopysetsBackupSelector.o \
    obj.javabindings/MockCluster.o \
    obj.javabindings/MockDriver.o \
    obj.javabindings/MockExternalStorage.o \
    obj.javabindings/MockInfiniband.o \
    obj.javabindings/MockTransport.o obj.javabindings/ObjectManager.o \
    obj.javabindings/OptionParser.o obj.javabindings/PingService.o \
    obj.javabindings/PriorityTaskQueue.o obj.javabindings/Recovery.o \
    obj.javabindings/RecoverySegmentBuilder.o \
    obj.javabindings/ReplicaManager.o obj.javabindings/ReplicatedSegment.o \
    obj.javabindings/RuntimeOptions.o obj.javabindings/SegmentIterator.o \
    obj.javabindings/SegmentManager.o obj.javabindings/Server.o \
    obj.javabindings/ServerListEntry.pb.o obj.javabindings/SideLog.o \
    obj.javabindings/SingleFileStorage.o obj.javabindings/Table.pb.o \
    obj.javabindings/TableManager.o obj.javabindings/TableManager.pb.o \
    obj.javabindings/TableStats.o obj.javabindings/Tablet.o \
    obj.javabindings/TabletManager.o obj.javabindings/TaskQueue.o \
    obj.javabindings/WallTime.o \
    -ldl -lpcrecpp -lboost_program_options -lprotobuf -lrt \
    -lboost_filesystem -lboost_system -lssl -lcrypto -rdynamic -libverbs
make clean
make -j12 DEBUG=no
