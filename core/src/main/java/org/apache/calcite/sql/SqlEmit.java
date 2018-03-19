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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

/**
 * A <code>SqlEmit</code> is a node of a parse tree which represents an EMIT
 * statement.
 */
public class SqlEmit extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("EMIT", SqlKind.EMIT);

  /**
   * The BEFORE_COMPLETE operator
   */
  static final SqlPostfixOperator BEFORE_COMPLETE_OPERATOR =
      new SqlPostfixOperator("BEFORE COMPLETE", SqlKind.PRECEDING, 20,
          ReturnTypes.ARG0, null, null);

  /**
   * The AFTER_COMPLETE operator
   */
  static final SqlPostfixOperator AFTER_COMPLETE_OPERATOR =
      new SqlPostfixOperator("AFTER COMPLETE", SqlKind.FOLLOWING, 20,
          ReturnTypes.ARG0, null, null);

  SqlNode beforeDelay;
  SqlNode afterDelay;

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   */
  public SqlEmit(SqlParserPos pos, SqlNode beforeDelay, SqlNode afterDelay) {
    super(pos);
    this.beforeDelay = beforeDelay;
    this.afterDelay = afterDelay;
  }

  /**
   * Returns delay of before complete in milli second. Returns 0 if no delay.
   */
  public long getBeforeDelayValue() {
    return getDelayValue(beforeDelay);
  }

  /**
   * Returns delay of after complete in milli second. Returns 0 if no delay.
   */
  public long getAfterDelayValue() {
    return getDelayValue(afterDelay);
  }

  public SqlNode getBeforeDelay() {
    return beforeDelay;
  }

  public SqlNode getAfterDelay() {
    return afterDelay;
  }

  private long getDelayValue(SqlNode delay) {
    if (delay == null) {
      return Long.MIN_VALUE;
    } else if (isWithoutDelay(delay)) {
      return 0L;
    } else {
      return ((SqlLiteral) delay).getValueAs(BigDecimal.class).longValue();
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(beforeDelay, afterDelay);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.newlineAndIndent();
    writer.keyword("EMIT");
    writer.newlineAndIndent();
    if (beforeDelay != null) {
      unparse(beforeDelay, writer);
      writer.keyword("BEFORE COMPLETE");
    }

    if (afterDelay != null) {
      if (beforeDelay != null) {
        writer.print(",");
        writer.newlineAndIndent();
      }
      unparse(afterDelay, writer);
      writer.keyword("AFTER COMPLETE");
    }
  }

  private void unparse(SqlNode delay, SqlWriter writer) {
    if (isWithoutDelay(delay)) {
      delay.unparse(writer, 0, 0);
    } else {
      writer.keyword("WITH DELAY");
      SqlIntervalLiteral interval = (SqlIntervalLiteral) delay;
      SqlIntervalLiteral.IntervalValue value =
          (SqlIntervalLiteral.IntervalValue) interval.getValue();
      writer.literal("'" + value.getIntervalLiteral() + "'");
      writer.keyword(value.getIntervalQualifier().toString());
    }
  }

  /**
   * An enumeration of types of delay in a emit strategy: <code>WITHOUT DELAY</code>.
   */
  enum Delay {
    WITHOUT_DELAY("WITHOUT DELAY");

    private final String sql;

    Delay(String sql) {
      this.sql = sql;
    }

    public String toString() {
      return sql;
    }

    /**
     * Creates a parse-tree node representing an occurrence of this delay
     * type at a particular position in the parsed text.
     */
    public SqlNode symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  public static SqlEmit create(SqlParserPos pos, List<SqlNode> strategies) {
    SqlNode beforeDelay = null;
    SqlNode afterDelay = null;
    for (SqlNode s : strategies) {
      if (SqlKind.PRECEDING == s.getKind() && beforeDelay == null) {
        beforeDelay = ((SqlCall) s).getOperandList().get(0);
      } else if (SqlKind.FOLLOWING == s.getKind() && afterDelay == null) {
        afterDelay = ((SqlCall) s).getOperandList().get(0);
      } else {
        throw new RuntimeException("Sql Emit statement shouldn't contain duplicate strategies");
      }
    }
    return new SqlEmit(pos, beforeDelay, afterDelay);
  }

  public static SqlNode createWithoutDelay(SqlParserPos pos) {
    return Delay.WITHOUT_DELAY.symbol(pos);
  }

  public static SqlNode createAfterStrategy(SqlNode delay, SqlParserPos pos) {
    return AFTER_COMPLETE_OPERATOR.createCall(pos, delay);
  }

  public static SqlNode createBeforeStrategy(SqlNode delay, SqlParserPos pos) {
    return BEFORE_COMPLETE_OPERATOR.createCall(pos, delay);
  }

  /**
   * Returns whether an expression represents the "WITHOUT DELAY" strategy.
   */
  public static boolean isWithoutDelay(SqlNode node) {
    if (node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) node;
      if (literal.getTypeName() == SqlTypeName.SYMBOL) {
        return literal.symbolValue(Delay.class) == Delay.WITHOUT_DELAY;
      }
    }
    return false;
  }
}

// End SqlEmit.java
