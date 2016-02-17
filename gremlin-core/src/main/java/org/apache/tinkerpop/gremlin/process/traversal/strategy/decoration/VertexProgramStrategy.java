/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private transient Function<Graph, GraphComputer> graphComputerFunction;

    private VertexProgramStrategy() {

    }

    public VertexProgramStrategy(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))   // VertexPrograms can only execute at the root level of a Traversal
            return;

        traversal.addTraverserRequirement(TraverserRequirement.BULK); // all graph computations require bulk

        if (null == this.graphComputerFunction) { // TOTAL HACK!
            traversal.setParent(new TraversalVertexProgramStep(EmptyTraversal.instance(), EmptyTraversal.instance()));
            ComputerVerificationStrategy.instance().apply(traversal);
            traversal.setParent(EmptyStep.instance());
            return;
        }

        // back propagate as()-labels off of vertex computing steps
        Step<?, ?> currentStep = traversal.getEndStep();
        final Set<String> currentLabels = new HashSet<>();
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof VertexComputing) {
                currentLabels.addAll(currentStep.getLabels());
                currentStep.getLabels().forEach(currentStep::removeLabel);
            } else {
                currentLabels.forEach(currentStep::addLabel);
                currentLabels.clear();
            }
            currentStep = currentStep.getPreviousStep();
        }

        // push GraphStep forward in the chain to reduce the number of TraversalVertexProgram compilations
        currentStep = traversal.getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof GraphStep && currentStep.getNextStep() instanceof VertexComputing) {
                int index = TraversalHelper.stepIndex(currentStep.getNextStep(), traversal);
                traversal.removeStep(currentStep);
                traversal.addStep(index, currentStep);
            }
            currentStep = currentStep.getNextStep();
        }


        // wrap all non-VertexComputing steps into a TraversalVertexProgramStep
        currentStep = traversal.getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            Traversal.Admin<?, ?> computerTraversal = new DefaultTraversal<>();
            Step<?, ?> firstLegalOLAPStep = getFirstLegalOLAPStep(currentStep);
            Step<?, ?> lastLegalOLAPStep = getLastLegalOLAPStep(currentStep);
            if (!(firstLegalOLAPStep instanceof EmptyStep)) {
                int index = TraversalHelper.stepIndex(firstLegalOLAPStep, traversal);
                TraversalHelper.removeToTraversal(firstLegalOLAPStep, lastLegalOLAPStep.getNextStep(), (Traversal.Admin) computerTraversal);
                final TraversalVertexProgramStep traversalVertexProgramStep = new TraversalVertexProgramStep(traversal, computerTraversal);
                traversal.addStep(index, traversalVertexProgramStep);
            }
            currentStep = traversal.getStartStep();
            while (!(currentStep instanceof EmptyStep)) {
                if (!(currentStep instanceof VertexComputing) && !lastTraversalVertexProgramDone(traversal))
                    break;
                currentStep = currentStep.getNextStep();
            }
        }
        // if the last vertex computing step is a TraversalVertexProgramStep convert to OLTP with ComputerResultStep
        TraversalHelper.getLastStepOfAssignableClass(VertexComputing.class, traversal).ifPresent(step -> {
            if (step instanceof TraversalVertexProgramStep) {
                final ComputerResultStep computerResultStep = new ComputerResultStep<>(traversal, true);
                ((TraversalVertexProgramStep) step).getGlobalChildren().get(0).getEndStep().getLabels().forEach(computerResultStep::addLabel);
                // labeling should happen in TraversalVertexProgram (perhaps MapReduce)
                TraversalHelper.insertAfterStep(computerResultStep, (Step) step, traversal);
            }
        });
        // if there is a dangling vertex computing step, add an identity traversal (solve this in the future with a specialized MapReduce)
        if (traversal.getEndStep() instanceof VertexComputing && !(traversal.getEndStep() instanceof TraversalVertexProgramStep)) {
            final TraversalVertexProgramStep traversalVertexProgramStep = new TraversalVertexProgramStep(traversal, __.identity().asAdmin());
            traversal.addStep(traversalVertexProgramStep);
            traversal.addStep(new ComputerResultStep<>(traversal, true));
        }
        // all vertex computing steps needs the graph computer function
        traversal.getSteps().stream().filter(step -> step instanceof VertexComputing).forEach(step -> ((VertexComputing) step).setGraphComputerFunction(this.graphComputerFunction));

        //System.out.println(traversal + "!!!!!!!!!!!");
    }

    private static Step<?, ?> getFirstLegalOLAPStep(Step<?, ?> currentStep) {
        while (!(currentStep instanceof EmptyStep)) {
            if (!(currentStep instanceof VertexComputing))
                return currentStep;
            currentStep = currentStep.getNextStep();
        }
        return EmptyStep.instance();
    }

    private static Step<?, ?> getLastLegalOLAPStep(Step<?, ?> currentStep) {
        while (currentStep instanceof VertexComputing)
            currentStep = currentStep.getNextStep();
        while (!(currentStep instanceof EmptyStep)) {
            if (ComputerVerificationStrategy.isStepInstanceOfEndStep(currentStep))
                return currentStep;
            if (currentStep instanceof VertexComputing)
                return currentStep.getPreviousStep();
            currentStep = currentStep.getNextStep();
        }
        return EmptyStep.instance();
    }

    private static boolean lastTraversalVertexProgramDone(final Traversal.Admin<?, ?> traversal) {
        Optional<TraversalVertexProgramStep> optional = TraversalHelper.getLastStepOfAssignableClass(TraversalVertexProgramStep.class, traversal);
        if (!optional.isPresent())
            return false;
        else
            return ComputerVerificationStrategy.isStepInstanceOfEndStep(optional.get().getGlobalChildren().get(0).getEndStep());
    }

    public static Optional<GraphComputer> getGraphComputer(final Graph graph, final TraversalStrategies strategies) {
        final Optional<TraversalStrategy<?>> optional = strategies.toList().stream().filter(strategy -> strategy instanceof VertexProgramStrategy).findAny();
        return optional.isPresent() ? Optional.of(((VertexProgramStrategy) optional.get()).graphComputerFunction.apply(graph)) : Optional.empty();
    }
}
