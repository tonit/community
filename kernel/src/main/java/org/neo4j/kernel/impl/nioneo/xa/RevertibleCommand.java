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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyIndexCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.RelationshipCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.RelationshipTypeCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

/**
 * A wrapper around a Command that provides an undo step for the XA command.
 * This has nothing to do with {@link XaCommand#rollback()}, which is there
 * for releasing resources in the case of a rollback of the encompassing
 * transaction.
 */
public abstract class RevertibleCommand<S extends CommonAbstractStore, R extends Abstract64BitRecord>
{
    private final Command<S, R> delegate;
    private final Command<S, R> previous;

    RevertibleCommand( Command<S, R> delegate )
    {
        this.delegate = delegate;
        this.previous = generatePrevious();
    }

    /**
     * Creates the Command that represents the previous state of the record
     * altered by the delegate or the closest possible thing.
     *
     * @return A Command that reverts the record of the delegate to the before
     *         state.
     */
    private Command<S,R> generatePrevious()
    {
        /*
         * So, there are two cases:
         * 1. The delegate record is already in the store - no problem, we just get the
         *     current record from the store and return that.
         * 2. The delegate record is not yet in the store. That means it isCreated() (or
         *     should be at any rate) so asking for the previous is risky since it puts us
         *     outside the store file. So we return a dummy not inUse() record - should be
         *     enough.
         */
        long recordIdOfInterest = delegate.record.getId();
        R theRecord = null;
        try
        {
            theRecord = (R) delegate.store.getRecord( recordIdOfInterest );
            return generatePreviousFromRecord(theRecord);
        }
        catch ( InvalidRecordException e )
        {
            /*
             * ok, not in use
             */
            assert delegate.record.isCreated();
            return generatePreviousFromRecord(recordIdOfInterest);
        }
    }

    protected Command<S, R> getDelegate()
    {
        return delegate;
    }

    protected Command<S, R> getPrevious()
    {
        return previous;
    }

    protected abstract Command<S, R> generatePreviousFromRecord( R record );

    protected abstract Command<S, R> generatePreviousFromRecord( long id );

    /*
     * Implementation for all Commands are below. Mainly boilerplate code
     */

    public static class NodeRevertibleCommand extends
            RevertibleCommand<NodeStore, NodeRecord>
    {
        NodeRevertibleCommand( Command.NodeCommand delegate )
        {
            super( delegate );
        }

        @Override
        protected Command.NodeCommand generatePreviousFromRecord(
                NodeRecord record )
        {
            assert record != null;
            return new Command.NodeCommand(
                    ( (NodeCommand) getDelegate() ).store, record );
        }

        @Override
        protected Command.NodeCommand generatePreviousFromRecord( long id )
        {
            return new Command.NodeCommand(
                    ( (NodeCommand) getDelegate() ).store, new NodeRecord( id ) );
        }
    }

    public static class RelationshipRevertibleCommand extends
            RevertibleCommand<RelationshipStore, RelationshipRecord>
    {
        public RelationshipRevertibleCommand(
                Command.RelationshipCommand delegate )
        {
            super( delegate );
        }

        @Override
        protected Command.RelationshipCommand generatePreviousFromRecord(
                RelationshipRecord record )
        {
            assert record != null;
            return new Command.RelationshipCommand(
                    ( (RelationshipCommand) getDelegate() ).store, record );
        }

        @Override
        protected Command.RelationshipCommand generatePreviousFromRecord(
                long id )
        {
            return new Command.RelationshipCommand(
                    ( (RelationshipCommand) getDelegate() ).store,
                    new RelationshipRecord( id,
                            Record.NO_PREV_RELATIONSHIP.intValue(),
                            Record.NO_NEXT_RELATIONSHIP.intValue(), -1 ) );
        }
    }

    public static class RelationshipTypeRevertibleCommand
            extends
            RevertibleCommand<RelationshipTypeStore, RelationshipTypeRecord>
    {
        public RelationshipTypeRevertibleCommand(
                Command.RelationshipTypeCommand delegate )
        {
            super( delegate );
        }

        @Override
        protected Command.RelationshipTypeCommand generatePreviousFromRecord(
                long id )
        {
            return new Command.RelationshipTypeCommand(
                    ( (RelationshipTypeCommand) getDelegate() ).store,
                    new RelationshipTypeRecord( (int) id ) );
        }

        @Override
        protected Command.RelationshipTypeCommand generatePreviousFromRecord(
                RelationshipTypeRecord record )
        {
            /*
             * Special case alert: RelationshipTypes are always created, never
             * deleted or updated and never rolled back. So for every RelationshipType
             * there shouldn't be a corresponding previous record - that would mean
             * that this record updates an existing one - no go.
             */
            throw new IllegalStateException(
                    record + " is a RelationshipTypeRecord updated by "
                            + getDelegate() + ". This cannot be happening." );
        }
    }

    public static class PropertyRevertibleCommand extends
            RevertibleCommand<PropertyStore, PropertyRecord>
    {
        public PropertyRevertibleCommand( Command.PropertyCommand delegate )
        {
            super( delegate );
        }

        @Override
        protected Command<PropertyStore, PropertyRecord> generatePreviousFromRecord(
                long id )
        {
             return new Command.PropertyCommand(
            ( (PropertyCommand) getDelegate() ).store,
                    new PropertyRecord( id ) );
        }

        @Override
        protected Command<PropertyStore, PropertyRecord> generatePreviousFromRecord(
                PropertyRecord record )
        {
            return new Command.PropertyCommand(
                    ( (PropertyCommand) getDelegate() ).store, record );
        }
    }

    public static class PropertyIndexRevertibleCommand extends
            RevertibleCommand<PropertyIndexStore, PropertyIndexRecord>
    {
        public PropertyIndexRevertibleCommand(
                Command.PropertyIndexCommand delegate )
        {
            super( delegate );
        }

        @Override
        protected Command<PropertyIndexStore, PropertyIndexRecord> generatePreviousFromRecord(
                long id )
        {
            return new Command.PropertyIndexCommand(
                    ( (PropertyIndexCommand) getDelegate() ).store,
                    ( new PropertyIndexRecord( (int) id ) ) );
        }

        @Override
        protected Command<PropertyIndexStore, PropertyIndexRecord> generatePreviousFromRecord(
                PropertyIndexRecord record )
        {
            return new Command.PropertyIndexCommand(
                    ( (PropertyIndexCommand) getDelegate() ).store, record );
        }
    }
}
