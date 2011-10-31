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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.Command;

/**
 * A simple holder of information for undoing a set of Commands, probably
 * all part of the same transaction. Can also be written out and read from
 * a log buffer.
 */
public class RevertibleXaTransaction
{
    private final List<XaCommand> commands;
    private final int identifier;
    private final long creationTime;

    public RevertibleXaTransaction( int identifier, long creationTime )
    {
        this.commands = new LinkedList<XaCommand>();
        this.identifier = identifier;
        this.creationTime = creationTime;
    }

    protected void addCommand( XaCommand command )
    {
        commands.add( command );
    }

    public int getIdentifier()
    {
        return identifier;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public void writeOut
    ( LogBuffer toWriteTo ) throws IOException
    {
        toWriteTo.putInt( getIdentifier() );
        toWriteTo.putLong( getCreationTime() );
        for ( XaCommand cmd : commands )
        {
            LogIoUtils.writeCommand( toWriteTo, getIdentifier(), cmd );
        }
        LogIoUtils.writeDone( toWriteTo, identifier );
    }

    /**
     * Given a ByteBuffer as scratch space and a channel that contains a
     * RevertibleTransaction, it returns the RevertibleTransaction that
     * was stored there, null if incomplete (no Done record)
     * 
     * @param buffer A buffer to use as scratch space
     * @param channel The channel that holds the RevertibleTransaction,
     *            positioned at its start.
     * @param store The NeoStore for command generation
     * @return The RevertibleTransaction held in the channel
     * @throws IOException
     */
    public static RevertibleXaTransaction readFrom( ByteBuffer buffer,
            ReadableByteChannel channel, NeoStore store ) throws IOException
    {
        buffer.limit( 4 + 8 );
        channel.read( buffer );
        buffer.flip();
        int identifier = buffer.getInt();
        long creationTime = buffer.getLong();
        RevertibleXaTransaction result = new RevertibleXaTransaction(
                identifier, creationTime );
        CommandFactory cmdFac = new CommandFactory(store);
        LogEntry current = null;

        while ( (current = LogIoUtils.readEntry( buffer, channel, cmdFac )) != null )
        {
            if (current instanceof LogEntry.Done)
            {
                return result;
            }
            else
            {
                result.addCommand( ( (LogEntry.Command) current ).getXaCommand() );
            }
        }
        return null;
    }

    private static class CommandFactory extends XaCommandFactory
    {
        private final NeoStore neoStore;

        CommandFactory( NeoStore neoStore )
        {
            this.neoStore = neoStore;
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            Command command = Command.readCommand( neoStore, byteChannel,
                    buffer );
            return command;
        }
    }
}
