/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;

/**
 * A <code>LogicalTemporalTableScan</code> reads the rows from a table for a point-in-time.
 */
public class LogicalTemporalTableScan extends TableScan {

  private final RexNode period;

  /**
   * Creates a LogicalTemporalTableScan.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTemporalTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, RexNode period) {
    super(cluster, traitSet, table);
    this.period = period;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return pw
        .item("table", table.getQualifiedName())
        .item("period", period);
  }

  public RexNode getPeriod() {
    return period;
  }

  public static LogicalTemporalTableScan create(RelOptCluster cluster,
                                                final RelOptTable relOptTable, RexNode period) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    if (table != null) {
                      return table.getStatistic().getCollations();
                    }
                    return ImmutableList.of();
                  }
                });
    return new LogicalTemporalTableScan(cluster, traitSet, relOptTable, period);
  }
}

// End LogicalTemporalTableScan.java
