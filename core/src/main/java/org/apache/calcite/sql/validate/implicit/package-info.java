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

/**
 * Sql implicit type cast.
 * <h2>Work Flow</h2>
 * This package contains rules for implicit type coercion, it works in the process of sql
 * validation. The transform entrance are all kinds of checkers. i.e.
 * {@link org.apache.calcite.sql.type.ImplicitCastInputTypesChecker},
 * {@link org.apache.calcite.sql.type.AssignableOperandTypeChecker},
 * {@link org.apache.calcite.sql.type.ComparableOperandTypeChecker}
 * {@link org.apache.calcite.sql.type.CompositeOperandTypeChecker},
 * {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker},
 * {@link org.apache.calcite.sql.type.SameOperandTypeChecker},
 * {@link org.apache.calcite.sql.type.SetopOperandTypeChecker}.
 *
 * <p> When user do a validation, and the type coercion is turned on(default), the validator will
 * check the operands/returned types of all kinds of Operators, if the validation passes through,
 * the validator will just cache the data type (say RelDataType) for the
 * {@link org.apache.calcite.sql.SqlNode} it has validated; If the validation fails, the validator
 * will ask the {@link org.apache.calcite.sql.validate.implicit.TypeCoercion} about if we can do
 * an implicit type coercion, if the coercion rules passed, the
 * {@link org.apache.calcite.sql.validate.implicit.TypeCoercion} component will replace the
 * {@link org.apache.calcite.sql.SqlNode} with a casted one, the node may be an operand of an
 * Operator or a column field of a selected row,
 * {@link org.apache.calcite.sql.validate.implicit.TypeCoercion} will then update the inferred type
 * for the casted node and the corresponding Operator/Row column type. If the coercion rule fails
 * again, the validator will just throw the exception as is before.</p>
 *
 * <p> For some cases, although the validation passed, we still need the type coercion, i.e. for
 * expression 1 > '1', Calcite will just return false without type coercion, we do type coercion
 * eagerly here and the result expression will be 1 > cast('1' as int) then the result would be
 * true.</p>
 *
 * <h2>Conversion Expressions</h2>
 * The conversion contexts we support now are:
 * <a href="https://docs.google.com/document/d/1g2RUnLXyp_LjUlO-wbblKuP5hqEu3a_2Mt2k4dh6RwU/edit?usp=sharing">Conversion Expressions</a>
 * <p>For finding common type strategies:</p>
 * <ul>
 *   <li>If the operator has expected data types, just take them as the desired one.</li>
 *   <li>If no expected data type but data type families are registered, try to coerce to
 *   the family's default data type, i.e. the String family will have a VARCHAR type.</li>
 *   <li>If neither expected data type nor families are specified, try to find the tightest common
 *   type of the node types, i.e. int and double will return double, the numeric precision does not
 *   lose for this case.</li>
 *   <li>If no tightest common type found, we will try to find a wider type, i.e. string and int
 *   will return int, we allow some precision loss when widening decimal to fractional,
 *   or promote to string type.
 *   </li>
 * </ul>
 *
 * <h2>Types Conversion Mapping</h2>
 * See <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">CalciteImplicitCasts</a>
 */
@PackageMarker
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
