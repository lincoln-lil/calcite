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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Default Strategies that can be used to coerce differing types that participate in
 * operations into compatible ones.
 * <p>Notes about type widening / tightest common types: Broadly, there are two cases when we need
 * to widen data types (e.g. set operations, binary comparison):</p>
 * <ul>
 * <li>Case 1: Looking for a common data type for two or more data types,
 * and no loss of precision is allowed. Examples include type inference for returned/operands
 * type (e.g. what's the column's data type if one row is an integer while the other row is a
 * long?).</li>
 * <li>Case2: Looking for a widened data type with some acceptable loss of precision
 * (e.g. there is no common type for double and decimal because double's range is larger than
 * decimal, and yet decimal is more precise than double, but in union we would cast the decimal
 * into double).</li>
 * </ul>
 * <p>REFERENCE: <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine?">SQL-SERVER</a>
 * <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types">HIVE</a></p>
 */
public interface TypeCoercion {
  /**
   * Case1: type widening with no precision loss.
   * Find the tightest common type of two types that might be used in binary expression.
   *
   * @return common type
   */
  RelDataType getTightestCommonType(RelDataType type1, RelDataType type2);

  /**
   * Case2: type widening. The main difference with
   * {@link #getTightestCommonType} is that we allow
   * some precision loss when widening decimal to fractional, or promote to string type.
   */
  RelDataType getWiderTypeForTwo(RelDataType type1, RelDataType type2, boolean stringPromotion);

  /**
   * Similar to {@link #getWiderTypeForTwo}, but can handle
   * sequence types. [[getWiderTypeForTwo]] doesn't satisfy the associative law,
   * i.e. (a op b) op c may not equal to a op (b op c). This is only a problem for StringType or
   * nested StringType in ArrayType. Excluding these types, [[getWiderTypeForTwo]] satisfies the
   * associative law. For instance,
   * (DATE, INTEGER, VARCHAR) should have VARCHAR as the wider common type.
   */
  RelDataType getWiderTypeFor(List<RelDataType> typeList, boolean stringPromotion);

  /**
   * Finds a wider type when one or both types are decimals. If the wider decimal type exceeds
   * system limitation, this rule will truncate the decimal type. If a decimal and other fractional
   * types are compared, returns a decimal type which has higher precision. This default
   * implementation depends on the max precision and scale of type system, you can override it
   * based on the specific cases.
   */
  RelDataType getWiderTypeForDecimal(RelDataType type1, RelDataType type2);

  /**
   * Determines common type for a comparison operator when one operand is String type and the
   * other is not. For date + timestamp operands we make the target type to be timestamp,
   * i.e. Timestamp(2017-01-01 00:00 ...) < Date(2018) = true.
   */
  RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2);

  /**
   * Widen a SqlNode ith column type to target type, this method mainly used for SqlQuery context.
   *
   * @param scope       scope to query
   * @param query       SqlNode which have children nodes as columns
   * @param columnIndex target column index
   * @param targetType  target type to cast to
   * @return true if we add any cast in successfully.
   */
  boolean widenColumnTypes(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType);

  /**
   * Handles type coercion for both IN expression with or without subquery.
   * see {@link TypeCoercionImpl} for default strategies.
   */
  boolean inOperationConversion(SqlCallBinding binding);

  /** Coerce string type operand in arithmetic expressions to Numeric type.*/
  boolean binaryArithmeticPromote(SqlCallBinding binding);

  /**
   * Coerce CASE WHEN statement branches to one common type.
   * <p>Rules:</p>
   * Look up all the then operands and else operands types then find the common wider type, then
   * try to coerce the then/else operands to the found type.
   * */
  boolean caseWhenCoercion(SqlCallBinding binding);

  /**
   * Type coercion based on the inferred type from passed in operand and the {@link SqlTypeFamily}
   * defined in the checkers, i.e. the {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker}.
   * Caution that We do not cast from numeric if desired type family is also
   * {@link SqlTypeFamily#NUMERIC}.
   * <p>If the FamilyOperandTypeChecker is subsumed in a
   * {@link org.apache.calcite.sql.type.CompositeOperandTypeChecker}, we will check it in order they
   * are placed for the composition. i.e. if we allows a numeric_numeric OR string_numeric family
   * but we pass in operands (op1, op2) of types (varchar(20), boolean), we will try to coerce op1
   * to numeric and op2 to numeric if the type coercion rules allow it, or we will try to coerce
   * op2 to numeric and keep op1 the type as it was before. This is also very interrelated to the
   * composition predicate here, if the predicate is AND, we would fail fast if the first
   * family type coercion fails.</p>
   * @param binding          call binding.
   * @param operandTypes     Types of the operands passed in.
   * @param expectedFamilies Expected SqlTypeFamily list by user specified.
   * @return if we successfully do any implicit cast.
   */
  boolean implicitTypeCast(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies);

  /**
   * Type coercion for not builtin functions, i.e. UDF. We will compare the types of arguments by
   * the rules below:
   * <ol>
   * <li>named param: find the desired type by the passed in operand's name.</li>
   * <li>non named param: find the desired type by formal param ordinal.</li>
   * </ol>
   * <p>If we find a desired type, we will try to make a type coercion, if we changed something
   * (that means we did a cast for some operands), will return true.</p>
   */
  boolean coerceFunctionParams(SqlValidatorScope scope, SqlCall call, SqlFunction function);
}

// End TypeCoercion.java
