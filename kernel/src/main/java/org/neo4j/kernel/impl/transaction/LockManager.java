/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.info.LockInfo;

public interface LockManager
{

    public abstract long getDetectedDeadlockCount();

    /**
     * Tries to acquire read lock on <CODE>resource</CODE> for the current
     * transaction. If read lock can't be acquired the transaction will wait for
     * the lransaction until it can acquire it. If waiting leads to dead lock a
     * {@link DeadlockDetectedException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws DeadlockDetectedException
     *             If a deadlock is detected
     * @throws IllegalResourceException
     */
    public abstract void getReadLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException;

    /**
     * Tries to acquire write lock on <CODE>resource</CODE> for the current
     * transaction. If write lock can't be acquired the transaction will wait
     * for the lock until it can acquire it. If waiting leads to dead lock a
     * {@link DeadlockDetectedException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws DeadlockDetectedException
     *             If a deadlock is detected
     * @throws IllegalResourceException
     */
    public abstract void getWriteLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException;

    /**
     * Releases a read lock held by the current transaction on <CODE>resource</CODE>.
     * If current transaction don't have read lock a
     * {@link LockNotFoundException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws IllegalResourceException
     * @throws LockNotFoundException
     */
    public abstract void releaseReadLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException;

    /**
     * Releases a read lock held by the current transaction on <CODE>resource</CODE>.
     * If current transaction don't have read lock a
     * {@link LockNotFoundException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws IllegalResourceException
     * @throws LockNotFoundException
     */
    public abstract void releaseWriteLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException;

    /**
     * Utility method for debugging. Dumps info to console of txs having locks
     * on resources.
     *
     * @param resource
     */
    public abstract void dumpLocksOnResource( Object resource );

    public abstract List<LockInfo> getAllLocks();

    public abstract List<LockInfo> getAwaitedLocks( long minWaitTime );

    /**
     * Utility method for debugging. Dumps the resource allocation graph to
     * console.
     */
    public abstract void dumpRagStack();

    /**
     * Utility method for debugging. Dumps info about each lock to console.
     */
    public abstract void dumpAllLocks();

}
