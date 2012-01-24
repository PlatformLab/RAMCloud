/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * This file implements the CoordinatorServerList class.
 */

#include "Common.h"
#include "CoordinatorServerList.h"
#include "ShortMacros.h"

namespace RAMCloud {

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Public Methods
//////////////////////////////////////////////////////////////////////

/**
 * Constructor for CoordinatorServerList.
 */
CoordinatorServerList::CoordinatorServerList()
    : serverList(),
      numberOfMasters(0),
      numberOfBackups(0),
      versionNumber(0)
{
}

/**
 * Destructor for CoordinatorServerList.
 */
CoordinatorServerList::~CoordinatorServerList()
{
}

/**
 * Add a new server to the CoordinatorServerList and generate a new, unique
 * ServerId for it.
 *
 * \param serviceLocator
 *      The ServiceLocator string of the server to add.
 *
 * \param serviceMask
 *      Which services this server supports.
 *
 * \param protoBuf
 *      Protocol Buffer to serialise the added entry in to. This message can
 *      then be sent along to other servers in the cluster, alerting them of
 *      this server's addition.
 *
 * \return
 *      The unique ServerId assigned to this server.
 */
ServerId
CoordinatorServerList::add(string serviceLocator,
                           ServiceMask serviceMask,
                           ProtoBuf::ServerList& protoBuf)
{
    uint32_t index = firstFreeIndex();

    ServerId id(index, serverList[index].nextGenerationNumber);
    serverList[index].nextGenerationNumber++;
    serverList[index].entry.construct(id, serviceLocator, serviceMask);

    if (serviceMask.has(MASTER_SERVICE))
        numberOfMasters++;
    if (serviceMask.has(BACKUP_SERVICE))
        numberOfBackups++;

    versionNumber++;
    ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
    serverList[index].entry->serialise(protoBufEntry, true);
    protoBuf.set_version_number(versionNumber);

    return id;
}

/**
 * Remove a server from the list, typically when it is no longer part of
 * the system and we don't care about it anymore (e.g. it crashed and has
 * been properly recovered).
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *
 * \param protoBuf
 *      Protocol Buffer to serialise the removed entry in to. This message
 *      can then be sent along to other servers in the cluster, alerting them
 *      of this server's removal.
 */
void
CoordinatorServerList::remove(ServerId serverId,
                              ProtoBuf::ServerList& protoBuf)
{
    uint32_t index = serverId.indexNumber();
    if (index < serverList.size() && serverList[index].entry &&
      serverList[index].entry->serverId == serverId) {
        if (serverList[index].entry->isMaster())
            numberOfMasters--;
        if (serverList[index].entry->isBackup())
            numberOfBackups--;

        versionNumber++;
        ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
        serverList[index].entry->serialise(protoBufEntry, false);
        protoBuf.set_version_number(versionNumber);

        serverList[index].entry.destroy();
        return;
    }

    throw Exception(HERE, format("Invalid ServerId (%lu)", serverId.getId()));
}

/**
 * \copydetails CoordinatorServerList::getReferenceFromServerId
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::operator[](const ServerId& serverId) const
{
    return getReferenceFromServerId(serverId);
}

/**
 * \copydetails CoordinatorServerList::getReferenceFromServerId
 */
CoordinatorServerList::Entry&
CoordinatorServerList::operator[](const ServerId& serverId)
{
    return const_cast<Entry&>(getReferenceFromServerId(serverId));
}

/**
 * \copydetails CoordinatorServerList::getPointerFromIndex
 */
const CoordinatorServerList::Entry*
CoordinatorServerList::operator[](size_t index) const
{
    return getPointerFromIndex(index);
}

/**
 * \copydetails CoordinatorServerList::getPointerFromIndex
 */
CoordinatorServerList::Entry*
CoordinatorServerList::operator[](size_t index)
{
    return const_cast<Entry*>(getPointerFromIndex(index));
}

/**
 * Return true if the given serverId is in this list. This can be used
 * to check membership, rather than having to try and catch around the
 * index operator.
 */
bool
CoordinatorServerList::contains(ServerId serverId) const
{
    uint32_t index = serverId.indexNumber();

    if (index >= serverList.size())
        return false;

    if (!serverList[index].entry)
        return false;

    return serverList[index].entry->serverId == serverId;
}

/**
 * Return the number of valid indexes in this list. Valid does not mean that
 * they're occupied, only that they are within the bounds of the array.
 */
size_t
CoordinatorServerList::size() const
{
    return serverList.size();
}

/**
 * Get the number of masters in the list.
 */
uint32_t
CoordinatorServerList::masterCount() const
{
    return numberOfMasters;
}

/**
 * Get the number of backups in the list.
 */
uint32_t
CoordinatorServerList::backupCount() const
{
    return numberOfBackups;
}

/**
 * Returns the next index greater than or equal to the given index
 * that describes a master server in the list. If there is no next
 * master or startIndex exceeds the list size, -1 is returned.
 */
uint32_t
CoordinatorServerList::nextMasterIndex(uint32_t startIndex) const
{
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isMaster())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Returns the next index greater than or equal to the given index
 * that describes a backup server in the list. If there is no next
 * backup or startIndex exceeds the list size, -1 is returned.
 */
uint32_t
CoordinatorServerList::nextBackupIndex(uint32_t startIndex) const
{
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isBackup())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Serialise the entire list to a Protocol Buffer form.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 */
void
CoordinatorServerList::serialise(ProtoBuf::ServerList& protoBuf) const
{
    serialise(protoBuf, {MASTER_SERVICE, BACKUP_SERVICE});
}

/**
 * Serialise this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 *
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialise(ProtoBuf::ServerList& protoBuf,
                                 ServiceMask services) const
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (!serverList[i].entry)
            continue;

        const Entry& entry = *serverList[i].entry;

        if ((entry.isMaster() && services.has(MASTER_SERVICE)) ||
            (entry.isBackup() && services.has(BACKUP_SERVICE))) {
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
            entry.serialise(protoBufEntry, true);
        }
    }

    protoBuf.set_version_number(versionNumber);
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

/**
 * Return the first free index in the server list. If the list is
 * completely full, resize it and return the next free one.
 *
 * Note that index 0 is reserved. This method must never return it.
 */
uint32_t
CoordinatorServerList::firstFreeIndex()
{
    // Naive, but probably fast enough for a good long while.
    size_t index;
    for (index = 1; index < serverList.size(); index++) {
        if (!serverList[index].entry)
            break;
    }

    if (index >= serverList.size())
        serverList.resize(index + 1);

    assert(index != 0);
    return downCast<uint32_t>(index);
}

/**
 * Obtain a reference to the entry associated with the given ServerId.
 *
 * \param serverId
 *      The ServerId to look up in the list.
 *
 * \throw
 *      An exception is thrown if the given ServerId is not in this list.
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::getReferenceFromServerId(const ServerId& serverId) const
{
    uint32_t index = serverId.indexNumber();
    if (index < serverList.size() && serverList[index].entry)
        return *serverList[index].entry;

    throw Exception(HERE, format("Invalid ServerId (%lu)", serverId.getId()));
}

/**
 * Obtain a pointer to the entry at the given index of the list. This can
 * be used to iterate over the entire list (in conjunction with the #size
 * method), or by . If there is no entry at the given index, NULL is returned.
 *
 * TODO(Rumble): Should this method always return NULL (i.e. not throw if the index
 *      is out of bounds)?.
 *
 * \param index
 *      The index of the entry to return, if there is one.
 *
 * \throw
 *      An exception is thrown if the index exceeds the length of the list.
 */
const CoordinatorServerList::Entry*
CoordinatorServerList::getPointerFromIndex(size_t index) const
{
    if (index >= serverList.size())
        throw Exception(HERE, format("Index beyond array length (%zd)", index));

    if (!serverList[index].entry)
        return NULL;

    return serverList[index].entry.get();
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList::Entry Methods
//////////////////////////////////////////////////////////////////////

/**
 * Construct a new Entry, which contains the data a coordinator
 * needs to maintain about an enlisted server.
 *
 * \param serverId
 *      The ServerId of the server this entry describes.
 *
 * \param serviceLocatorString
 *      The ServiceLocator string that can be used to address this
 *      entry's server.
 *
 * \param serviceMask
 *      Which services this server supports.
 */
CoordinatorServerList::Entry::Entry(ServerId serverId,
                                    string serviceLocatorString,
                                    ServiceMask serviceMask)
    : serverId(serverId),
      serviceLocator(serviceLocatorString),
      serviceMask(serviceMask),
      will(NULL),
      backupReadMegsPerSecond(0)
{
}

/**
 * Serialise this entry into the given ProtoBuf.
 */
void
CoordinatorServerList::Entry::serialise(ProtoBuf::ServerList_Entry& dest,
                                        bool isInCluster) const
{
    dest.set_service_mask(serviceMask.serialize());
    dest.set_server_id(serverId.getId());
    dest.set_service_locator(serviceLocator);
    dest.set_is_in_cluster(isInCluster);
    if (isBackup())
        dest.set_user_data(backupReadMegsPerSecond);
    else
        dest.set_user_data(0);          // Tests expect the field to be present.
}

} // namespace RAMCloud
