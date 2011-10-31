/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa.undo;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.xa.RevertibleCommand;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;

/**
 * A repository for transaction undo entries. Essentially a chopped down version
 * of the XaLogicalLog, without the rollback,recovery or injection facilities.
 * Supposed to hold transactions that are just about to be committed.
 * <p>
 * In particular, when a commit is called on a transaction, the start entry for
 * the undo record is written with the same timestamp and identifier. When each
 * command is applied, the corresponding revertible command (i.e. the previous
 * state of the affected record) is recorded here. Finally, when the commit
 * entry is written, a done entry is recorded here as well, to signify on a
 * future scan that this undo record is complete.
 */
public class UndoLog
{
    private LogBuffer writeBuffer;
    private final LogBufferFactory logBufferFactory;
    public static final long FILE_SIZE_LIMIT = 3;

    public UndoLog( String filename, Map<?, ?> config ) throws IOException
    {
        this.logBufferFactory = (LogBufferFactory) config.get( LogBufferFactory.class );
        FileChannel channel = new RandomAccessFile( filename, "rw" ).getChannel();
        writeBuffer = logBufferFactory.create( channel );
    }

    public void writeCommand( RevertibleCommand<?, ?> toWrite,
            int identifier ) throws IOException
    {
        LogIoUtils.writeCommand( writeBuffer, identifier, toWrite.getPrevious() );
    }

    public void start( int identifier, Xid xid, int masterId, long creationTime )
            throws IOException
    {
        LogIoUtils.writeStart( writeBuffer, identifier, xid, masterId,
                creationTime );
    }

    public void done( int identifier ) throws IOException
    {
        LogIoUtils.writeDone( writeBuffer, identifier );
        writeBuffer.force();
        checkRotation();
    }

    private void checkRotation() throws IOException
    {
        if ( writeBuffer.getFileChannelPosition() > FILE_SIZE_LIMIT )
        {

        }
    }

    /**
     * Writes out a trailing table of timestamp to offset of start entry for
     * quick lookup for point-in-time recovery. At the end there is a canary
     * value that signifies the correct closing of the file a la
     * CommonAbstractStore.
     */
    public void writeFooter()
    {
    }
}
