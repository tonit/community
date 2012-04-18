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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class TestStopReplaceDbStart
{
    @Test
    public void testIt() throws Exception
    {
        File otherDb = produceDb();
        
        File path = TargetDirectory.forTest( getClass() ).graphDbDir( true );
        RestartableDb db = new RestartableDb( path.getAbsolutePath(), MapUtil.stringMap() );
        markWithName( db, "New" );
        System.out.println( "name before switch: " + getName( db ) );
        db.stop();
        
        deleteAllButMessageLog( path );
        FileUtils.copyRecursively( otherDb, path );
        
        db.startAgain();
        System.out.println( "name after switch: " + getName( db ) );
        db.shutdown();
    }

    private void deleteAllButMessageLog( File path ) throws IOException
    {
        for ( File file : path.listFiles() )
        {
            if ( file.getName().equals( StringLogger.DEFAULT_NAME ) )
                continue;
            FileUtils.deleteRecursively( file );
        }
    }

    private String getName( GraphDatabaseService db )
    {
        return (String) db.getReferenceNode().getProperty( "name" );
    }

    private void markWithName( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        db.getReferenceNode().setProperty( "name", name );
        tx.success();
        tx.finish();
    }
    
    private File produceDb() throws IOException
    {
        File path = new File( "target/test-data/produced-db" );
        FileUtils.deleteRecursively( path );
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( path.getAbsolutePath() );
        markWithName( db, "Produced" );
        db.shutdown();
        return path;
    }

    private static class RestartableDb extends AbstractGraphDatabase
    {
        public RestartableDb( String storeDir, Map<String, String> params )
        {
            super( storeDir, params, Service.load( IndexProvider.class ), Service.load( KernelExtension.class ),
                    Service.load( CacheProvider.class ) );
            run();
        }
        
        public void stop()
        {
            life.stop();
        }
        
        public void startAgain()
        {
            life.start();
        }
    }
}
