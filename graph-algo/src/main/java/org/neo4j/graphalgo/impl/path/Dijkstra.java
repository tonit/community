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
package org.neo4j.graphalgo.impl.path;

import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.kernel.StandardExpander.toPathExpander;
import static org.neo4j.kernel.Traversal.traversal;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory;
import org.neo4j.graphalgo.impl.util.StopAfterWeightIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Uniqueness;

/**
 * @author Tobias Ivarsson
 * @author Martin Neumann
 * @author Mattias Persson
 */
public class Dijkstra implements PathFinder<WeightedPath>
{
    private static final TraversalDescription TRAVERSAL = traversal().uniqueness( Uniqueness.NONE );

    private final PathExpander expander;
    private final CostEvaluator<Double> costEvaluator;
    private Traverser lastTraverser;

    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
    }

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( toPathExpander( expander ), costEvaluator );
    }
    
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        lastTraverser = TRAVERSAL.expand( expander ).order(
                new SelectorFactory( costEvaluator ) ).evaluator( Evaluators.includeWhereEndNodeIs( end ) ).traverse( start );
        
        // Here's how the bidirectional equivalent would look
//        lastTraverser = Traversal.bidirectionalTraversal()
//                .mirroredSides( TRAVERSAL.expand( expander ).order( new SelectorFactory( costEvaluator ) ) )
//                .traverse( start, end );
        
        return new Iterable<WeightedPath>()
        {
            public Iterator<WeightedPath> iterator()
            {
                return new StopAfterWeightIterator( lastTraverser.iterator(),
                        costEvaluator );
            }
        };
    }

    public WeightedPath findSinglePath( Node start, Node end )
    {
        return firstOrNull( findAllPaths( start, end ) );
    }
    
    @Override
    public TraversalMetadata metadata()
    {
        return lastTraverser.metadata();
    }
    
    private static class SelectorFactory extends BestFirstSelectorFactory<Double, Double>
    {
        private final CostEvaluator<Double> evaluator;

        SelectorFactory( CostEvaluator<Double> evaluator )
        {
            this.evaluator = evaluator;
        }

        @Override
        protected Double calculateValue( TraversalBranch next )
        {
            return next.length() == 0 ? 0d : evaluator.getCost(
                    next.lastRelationship(), Direction.OUTGOING );
        }

        @Override
        protected Double addPriority( TraversalBranch source,
                Double currentAggregatedValue, Double value )
        {
            return withDefault( currentAggregatedValue, 0d ) + withDefault( value, 0d );
        }

        private <T> T withDefault( T valueOrNull, T valueIfNull )
        {
            return valueOrNull != null ? valueOrNull : valueIfNull;
        }

        @Override
        protected Double getStartData()
        {
            return 0d;
        }
    }
}
