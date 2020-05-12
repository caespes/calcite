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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/** Implementation of {@link Correlate} in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableParallelCorrelate extends Correlate
    implements EnumerableRel {

  public EnumerableParallelCorrelate(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints,
      RelNode left, RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType) {
    super(cluster, traits, hints, left, right, correlationId, requiredColumns,
        joinType);
  }

  /** Creates an EnumerableCorrelate. */
  public static EnumerableParallelCorrelate create(
      RelNode left,
      RelNode right,
      List<RelHint> hints,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableCorrelate(mq, left, right, joinType));
    return new EnumerableParallelCorrelate(
        cluster,
        traitSet,
        hints,
        left,
        right,
        correlationId,
        requiredColumns,
        joinType);
  }

  @Override public EnumerableParallelCorrelate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType) {
    return new EnumerableParallelCorrelate(getCluster(),
        traitSet, hints, left, right, correlationId, requiredColumns, joinType);
  }

  public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    Expression leftExpression =
        builder.append(
            "left", leftResult.block);

    final BlockBuilder corrBlock = new BlockBuilder();
    Type corrVarType = leftResult.physType.getJavaRowType();
    ParameterExpression corrRef; // correlate to be used in inner loop
    ParameterExpression corrArg; // argument to correlate lambda (must be boxed)
    if (!Primitive.is(corrVarType)) {
      corrArg =
          Expressions.parameter(Modifier.FINAL,
              corrVarType, getCorrelVariable());
      corrRef = corrArg;
    } else {
      corrArg =
          Expressions.parameter(Modifier.FINAL,
              Primitive.box(corrVarType), "$box" + getCorrelVariable());
      corrRef = (ParameterExpression) corrBlock.append(getCorrelVariable(),
          Expressions.unbox(corrArg));
    }

    implementor.registerCorrelVariable(getCorrelVariable(), corrRef,
        corrBlock, leftResult.physType);

    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);

    implementor.clearCorrelVariable(getCorrelVariable());

    corrBlock.add(rightResult.block);

    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));

    Expression selector =
        EnumUtils.joinSelector(
            joinType, physType,
            ImmutableList.of(leftResult.physType, rightResult.physType));

    int parallel = 10;
    RelHint hint = this.getHints().stream()
        .filter(e -> e.hintName.equals("CORRELATE_PARALLEL"))
        .findFirst().orElse(null);

    if (hint != null && hint.kvOptions.containsKey("P")) {
      try {
        int p = Integer.parseInt(hint.kvOptions.get("P"));
        if (p > 0) {
          parallel = p;
        }
      } catch (Exception ignored) {
      }
    }

    builder.append(
        Expressions.call(leftExpression, BuiltInMethod.CORRELATE_PARALLEL_JOIN.method,
            Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
            Expressions.lambda(corrBlock.toBlock(), corrArg),
            selector, Expressions.constant(parallel)));

    return implementor.result(physType, builder.toBlock());
  }
}
