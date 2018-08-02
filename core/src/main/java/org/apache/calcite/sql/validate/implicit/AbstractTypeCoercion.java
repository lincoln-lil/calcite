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

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for all the type coercion, extending of this class is not necessary, but will have
 * some convenient methods.
 * <p>We make tool methods: {@link #coerceIthOperandTo}, {@link #coerceIthColumnTo},
 * {@link #needToCast}, {@link #updateInferredRowType},
 * {@link #updateInferredTypesFor}, {@link #updateInferredRowType}
 * all be seen by derived classes, so you can define system specific type coercion logic.
 * Caution that these methods may modify the {@link SqlNode} tree, you should know what you are
 * doing when extending your logic.</p>
 * <p>This class also defines the default implementation of the type widening, see
 * {@link TypeCoercion} doc and methods: {@link #getTightestCommonType}, {@link #getWiderTypeFor},
 * {@link #getWiderTypeForTwo}, {@link #getWiderTypeForDecimal},
 * {@link #commonTypeForBinaryComparison} for the detail strategies.</p>
 */
public abstract class AbstractTypeCoercion implements TypeCoercion {
  protected SqlValidator validator;
  protected RelDataTypeFactory factory;

  //~ Constructors -----------------------------------------------------------
  AbstractTypeCoercion(SqlValidator validator) {
    Objects.requireNonNull(validator);
    this.validator = validator;
    this.factory = validator.getTypeFactory();
  }
  //~ Methods ----------------------------------------------------------------

  public RelDataTypeFactory getFactory() {
    return this.factory;
  }

  public SqlValidator getValidator() {
    return this.validator;
  }

  /**
   * Cast ith operand to target type, we do this base on the fact that
   * validate happens before type coercion.
   */
  protected boolean coerceIthOperandTo(
      SqlValidatorScope scope,
      SqlCall call,
      int index,
      RelDataType targetType) {
    SqlNode node1 = call.getOperandList().get(index);
    // Check it early.
    if (!needToCast(scope, node1, targetType)) {
      return false;
    }
    // fix nullable attr.
    RelDataType targetType1 = syncAttributes(validator.deriveType(scope, node1), targetType);
    SqlNode desired = castTo(node1, targetType1);
    call.setOperand(index, desired);
    updateInferredTypesFor(desired, targetType1);
    return true;
  }


  /**
   * Cast ith column to target type.
   *
   * @param scope      validator scope for the node list
   * @param nodeList   column node list
   * @param index      index of column
   * @param targetType target type to cast to
   */
  protected boolean coerceIthColumnTo(
      SqlValidatorScope scope,
      SqlNodeList nodeList,
      int index,
      RelDataType targetType) {
    // This will happen when there is a star/dynamic-star column in the select list, and the source
    // is values, i.e. `select * from (values(1, 2, 3))`. We just return and do nothing here, but
    // update the inferred row type, then we will add in type cast when expanding star/dynamic-star.
    // See: SqlToRelConverter#convertSelectList for details.
    if (index >= nodeList.getList().size()) {
      // If we get here, we already decided there is a star(*) in the column,
      // just return true.
      return true;
    }

    final SqlNode node = nodeList.get(index);
    if (node instanceof SqlIdentifier) {
      // We do not expand a star/dynamic table col now.
      SqlIdentifier node1 = (SqlIdentifier) node;
      if (node1.isStar()) {
        return true;
      } else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
        // Todo: support implicit cast for dynamic table.
        return false;
      }
    }

    if (node instanceof SqlCall) {
      SqlCall node2 = (SqlCall) node;
      if (node2.getOperator().kind == SqlKind.AS) {
        final SqlNode operand = node2.operand(0);
        if (!needToCast(scope, operand, targetType)) {
          return false;
        }
        RelDataType targetType2 = syncAttributes(validator.deriveType(scope, operand), targetType);
        final SqlNode casted = castTo(operand, targetType2);
        node2.setOperand(0, casted);
        updateInferredTypesFor(casted, targetType2);
        return true;
      }
    }
    if (!needToCast(scope, node, targetType)) {
      return false;
    }
    RelDataType targetType3 = syncAttributes(validator.deriveType(scope, node), targetType);
    final SqlNode node3 = castTo(node, targetType3);
    nodeList.set(index, node3);
    updateInferredTypesFor(node3, targetType3);
    return true;
  }

  /**
   * Sync the data type additional attributes before casting, i.e. nullability, charset, collation.
   */
  RelDataType syncAttributes(
      RelDataType type1,
      RelDataType targetType) {
    RelDataType targetType1 = targetType;
    if (type1 != null) {
      targetType1 = factory.createTypeWithNullability(targetType1, type1.isNullable());
      if (SqlTypeUtil.inCharOrBinaryFamilies(type1)
          && SqlTypeUtil.inCharOrBinaryFamilies(targetType)) {
        Charset charset1 = type1.getCharset();
        SqlCollation collation1 = type1.getCollation();
        if (charset1 != null && SqlTypeUtil.inCharFamily(targetType1)) {
          targetType1 = factory.createTypeWithCharsetAndCollation(targetType1, charset1,
              collation1);
        }
      }
    }
    return targetType1;
  }

  /** Decide if a SqlNode should be cast to target type, derived class can override this strategy.*/
  protected boolean needToCast(SqlValidatorScope scope, SqlNode node1, RelDataType targetType) {
    RelDataType type1 = validator.deriveType(scope, node1);
    // This depends on the fact that type validate happens before coercion.
    // We do not have inferred type for some node, e.g. LOCALTIME.
    if (type1 == null) {
      return false;
    }

    // This prevents that we cast a JavaType to normal RelDataType.
    if (targetType.getSqlTypeName() == type1.getSqlTypeName()) {
      return false;
    }

    // We do not make a cast when don't know specific type (ANY) of the origin node.
    if (targetType.getSqlTypeName() == SqlTypeName.ANY
        || type1.getSqlTypeName() == SqlTypeName.ANY) {
      return false;
    }
    // Little promotion for character types,
    // No need to cast from char to varchar
    if (targetType.getSqlTypeName() == SqlTypeName.VARCHAR
        && type1.getSqlTypeName() == SqlTypeName.CHAR
        || targetType.getSqlTypeName() == SqlTypeName.CHAR
        && type1.getSqlTypeName() == SqlTypeName.VARCHAR) {
      return false;
    }
    // Little promotion for integer types,
    // i.e. we do not cast from tinyint to int or int to bigint.
    if (type1.getPrecedenceList().containsType(targetType)
        && SqlTypeUtil.isIntType(type1)
        && SqlTypeUtil.isIntType(targetType)) {
      return false;
    }
    // should keep sync with rules in [[SqlTypeAssignmentRules]].
    return !SqlTypeUtil.equalSansNullability(factory, type1, targetType);
  }

  // Reviewed chenyuzhao 2018-05-15: should consider constant inline ? No, we should have a
  // correct plan.
  /** It should not be used directly, cause some other work should be done before cast operation,
   * see {@link #coerceIthColumnTo} {@link #coerceIthOperandTo}. */
  SqlNode castTo(SqlNode node, RelDataType type) {
    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node,
        SqlTypeUtil.convertTypeToSpec(type));
  }

  /**
   * Update inferred type for a SqlNode.
   */
  protected void updateInferredTypesFor(SqlNode node, RelDataType type) {
    validator.setValidatedNodeType(node, type);
    final SqlValidatorNamespace namespace = validator.getNamespace(node);
    if (namespace != null) {
      namespace.setType(type);
    }
  }

  /**
   * Update inferred row type for a query like SqlCall or SqlSelect.
   *
   * @param scope       validator scope
   * @param query       node to inferred type
   * @param columnIndex index to update
   * @param targetType1 desired column type
   */
  protected void updateInferredRowType(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType1) {
    final RelDataType rowType = validator.deriveType(scope, query);
    assert rowType.isStruct();

    final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final RelDataTypeField field = rowType.getFieldList().get(i);
      final String name = field.getName();
      final RelDataType type = field.getType();
      final RelDataType targetType = i == columnIndex ? targetType1 : type;
      fieldList.add(Pair.of(name, targetType));
    }
    updateInferredTypesFor(query, factory.createStructType(fieldList));
  }

  /**
   * Case1: type widening with no precision loss.
   * Find the tightest common type of two types that might be used in binary expression.
   *
   * @return tightest common type i.e. INTEGER + DECIMAL(10, 2) will return DECIMAL(12, 2).
   */
  public RelDataType getTightestCommonType(RelDataType type1, RelDataType type2) {
    if (type1 == null || type2 == null) {
      return null;
    }
    // If just nullable is different, return type with nullable true.
    if (type1.equals(type2)
        || (type1.isNullable() != type2.isNullable()
        && factory.createTypeWithNullability(type1, type2.isNullable()).equals(type2))) {
      return factory.createTypeWithNullability(type1,
          type1.isNullable() || type2.isNullable());
    }

    // Null: return the other.
    if (SqlTypeUtil.isNull(type1)) {
      return type2;
    }
    if (SqlTypeUtil.isNull(type2)) {
      return type1;
    }
    RelDataType resultType = null;
    if (SqlTypeUtil.isString(type1)
        && SqlTypeUtil.isString(type2)) {
      resultType = factory.leastRestrictive(ImmutableList.of(type1, type2));
    }
    // Numeric: promote to highest type. i.e. SQL-SERVER/MYSQL supports numeric types cast from/to
    // each other.
    if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
      // For fixed-precision decimals interacting with each other or with other numeric types,
      // we want the operator to decide the precision and scale of the result.
      if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
        resultType = factory.leastRestrictive(ImmutableList.of(type1, type2));
      }
    }
    // Date + Timestamp: Timestamp.
    if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
      resultType = type2;
    }
    if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
      resultType = type1;
    }

    if (type1.isStruct() && type2.isStruct()) {
      if (SqlTypeUtil.equalAsStructSansNullability(factory, type1, type2, false)) {
        // Only name case and nullability may be different.
        // - Different names: use f1.name
        // - Different nullabilities: `nullable` is true if one of them is nullable.
        List<RelDataType> fields = new ArrayList<>();
        List<String> fieldNames = type1.getFieldNames();
        for (Pair<RelDataTypeField, RelDataTypeField> pair
            : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
          RelDataType leftType = pair.left.getType();
          RelDataType rightType = pair.right.getType();
          RelDataType dataType = getTightestCommonType(leftType, rightType);
          boolean isNullable = leftType.isNullable() || rightType.isNullable();
          fields.add(factory.createTypeWithNullability(dataType, isNullable));
        }
        return factory.createStructType(type1.getStructKind(), fields, fieldNames);
      }
    }

    if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
      if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
        resultType = factory.createTypeWithNullability(type1,
            type1.isNullable() || type2.isNullable());
      }
    }

    if (SqlTypeUtil.isMap(type1) && SqlTypeUtil.isMap(type2)) {
      if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
        RelDataType keyType = getTightestCommonType(type1.getKeyType(), type2.getKeyType());
        RelDataType valType = getTightestCommonType(type1.getValueType(), type2.getValueType());
        resultType = factory.createMapType(keyType, valType);
      }
    }

    return resultType;
  }

  /**
   * Promote all the way to VARCHAR.
   */
  private RelDataType promoteToVarChar(RelDataType type1, RelDataType type2) {
    RelDataType resultType = null;
    // No promotion for char and varchar.
    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isCharacter(type2)) {
      return null;
    }
    // We do not distinguish CHAR and VARCHAR, i.e. 1 > '1' will finally have type '1' > '1',
    // VARCHAR has 65536 as default precision.
    // Same as SPARK and SQL-SERVER: we support coerce binary or boolean to varchar.
    if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
      resultType = factory.createSqlType(SqlTypeName.VARCHAR);
    }

    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
      resultType = factory.createSqlType(SqlTypeName.VARCHAR);
    }
    return resultType;
  }

  /**
   * Determines common type for a comparison operator when one operand is String type and the
   * other is not. For date + timestamp operands we make the target type to be date,
   * i.e. Timestamp(2017-01-01 00:00 ...) < Date(2018) = true.
   */
  public RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2) {
    SqlTypeName typeName1 = type1.getSqlTypeName();
    SqlTypeName typeName2 = type2.getSqlTypeName();

    if (typeName1 == null || typeName2 == null) {
      return null;
    }

    if (SqlTypeUtil.isString(type1) && SqlTypeUtil.isDatetime(type2)
        || SqlTypeUtil.isDatetime(type1) && SqlTypeUtil.isString(type2)) {
      // instead of varchar we return null here,
      // cause calcite will do the cast in SqlToRelConverter.
      return null;
    }

    // date + timestamp -> timestamp
    if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
      return type2;
    }

    if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
      return type1;
    }

    if (SqlTypeUtil.isString(type1) && typeName2 == SqlTypeName.NULL) {
      return type1;
    }

    if (typeName1 == SqlTypeName.NULL && SqlTypeUtil.isString(type2)) {
      return type2;
    }

    if (SqlTypeUtil.isDecimal(type1) && SqlTypeUtil.isCharacter(type2)
        || SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isDecimal(type2)) {
      // There is no proper decimal type here, using double type
      // as the best we can do.
      return factory.createSqlType(SqlTypeName.DOUBLE);
    }

    // Keep sync with SQL-SERVER:
    // binary/varbinary can not cast to float/real/double cause precision loss,
    // character -> timestamp need explicit cast cause TZ.
    // Hive:
    // binary can not cast to anyone else,
    // character only to double/decimal.
    if (SqlTypeUtil.isBinary(type2) && SqlTypeUtil.isApproximateNumeric(type1)
        || SqlTypeUtil.isBinary(type1) && SqlTypeUtil.isApproximateNumeric(type2)) {
      return null;
    }

    // 1 > '1' will be coerced to 1 > 1.
    if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
      if (SqlTypeUtil.isTimestamp(type1)) {
        return null;
      }
      return type1;
    }

    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
      if (SqlTypeUtil.isTimestamp(type2)) {
        return null;
      }
      return type2;
    }

    return null;
  }

  /**
   * Case2: type widening. The main difference with
   * {@link #getTightestCommonType} is that we allow
   * some precision loss when widening decimal to fractional, or promote to string type.
   */
  public RelDataType getWiderTypeForTwo(
      RelDataType type1,
      RelDataType type2,
      boolean stringPromotion) {
    RelDataType resultType = getTightestCommonType(type1, type2);
    if (null == resultType) {
      resultType = getWiderTypeForDecimal(type1, type2);
    }
    if (null == resultType && stringPromotion) {
      resultType = promoteToVarChar(type1, type2);
    }
    if (null == resultType) {
      if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
        RelDataType valType = getWiderTypeForTwo(type1.getComponentType(),
            type2.getComponentType(), stringPromotion);
        if (null != valType) {
          resultType = factory.createArrayType(valType, -1);
        }
      }
    }
    return resultType;
  }

  /**
   * Finds a wider type when one or both types are decimals. If the wider decimal type exceeds
   * system limitation, this rule will truncate the decimal type. If a decimal and other fractional
   * types are compared, returns a decimal type which has higher precision.
   */
  public RelDataType getWiderTypeForDecimal(RelDataType type1, RelDataType type2) {
    if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
      return null;
    }
    // For Calcite DECIMAL has default max allowed precision, so just return decimal type.
    // This is based on the TypeSystem implementation, subclass should override it correctly.
    if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
      return factory.leastRestrictive(Lists.newArrayList(type1, type2));
    }
    return null;
  }

  /**
   * Similar to {@link #getWiderTypeForTwo}, but can handle
   * sequence types. [[getWiderTypeForTwo]] doesn't satisfy the associative law,
   * i.e. (a op b) op c may not equal to a op (b op c). This is only a problem for StringType or
   * nested StringType in ArrayType. Excluding these types, [[getWiderTypeForTwo]] satisfies the
   * associative law. For instance,
   * (DATE, INTEGER, VARCHAR) should have VARCHAR as the wider common type.
   */
  public RelDataType getWiderTypeFor(List<RelDataType> typeList, boolean stringPromotion) {
    assert typeList.size() > 1;
    RelDataType resultType = typeList.get(0);

    List<RelDataType> target = stringPromotion ? partitionByCharacter(typeList) : typeList;
    for (RelDataType tp : target) {
      resultType = getWiderTypeForTwo(tp, resultType, stringPromotion);
      if (null == resultType) {
        return null;
      }
    }
    return resultType;
  }

  private List<RelDataType> partitionByCharacter(List<RelDataType> types) {
    List<RelDataType> withCharacterTypes = new ArrayList<>();
    List<RelDataType> nonCharacterTypes = new ArrayList<>();

    for (RelDataType tp : types) {
      if (SqlTypeUtil.hasCharacter(tp)) {
        withCharacterTypes.add(tp);
      } else {
        nonCharacterTypes.add(tp);
      }
    }

    List<RelDataType> partitioned = new ArrayList<>();
    partitioned.addAll(withCharacterTypes);
    partitioned.addAll(nonCharacterTypes);
    return partitioned;
  }

  /**
   * Check if the types and families need to have a implicit type cast. We will check the type one
   * by one, that means the 1th type and 1th family, 2th type and 2th family, and the like.
   *
   * @param types    data type need to check.
   * @param families desired type families list.
   * @return true if we can do type coercion.
   */
  boolean canImplicitTypeCast(List<RelDataType> types, List<SqlTypeFamily> families) {
    boolean needed = false;
    if (types.size() != families.size()) {
      return false;
    }
    for (Pair<RelDataType, SqlTypeFamily> pair : Pair.zip(types, families)) {
      RelDataType implicitType = implicitCast(pair.left, pair.right);
      if (null == implicitType) {
        return false;
      }
      needed = pair.left != implicitType || needed;
    }
    return needed;
  }

  /**
   * Type coercion based on the inferred type from passed in operand and the {@link SqlTypeFamily}
   * defined in the checkers, i.e. the {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker}.
   * Caution that We do not cast from numeric and numeric.
   * See <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">
   * CalciteImplicitCasts</a> for the details.
   *
   * @param in       inferred operand type.
   * @param expected expected {@link SqlTypeFamily} of registered FUNC.
   * @return common type of implicit cast, null if we do not find one.
   */
  public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
    List<SqlTypeFamily> numericFamilies = Lists.newArrayList(
        SqlTypeFamily.NUMERIC,
        SqlTypeFamily.DECIMAL,
        SqlTypeFamily.APPROXIMATE_NUMERIC,
        SqlTypeFamily.EXACT_NUMERIC,
        SqlTypeFamily.INTEGER);
    List<SqlTypeFamily> dateTimeFamilies = Lists.newArrayList(SqlTypeFamily.DATE,
        SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP);
    // If the expected type is already a parent of the input type, no need to cast.
    if (expected.getTypeNames().contains(in.getSqlTypeName())) {
      return in;
    }
    // Cast null type (usually from null literals) into target types.
    if (SqlTypeUtil.isNull(in)) {
      return expected.getDefaultConcreteType(factory);
    }
    if (SqlTypeUtil.isNumeric(in) && expected == SqlTypeFamily.DECIMAL) {
      return factory.decimalOf(in);
    }
    // float/double -> decimal
    if (SqlTypeUtil.isApproximateNumeric(in) && expected == SqlTypeFamily.EXACT_NUMERIC) {
      return factory.decimalOf(in);
    }
    // Implicit cast date to timestamp type.
    if (SqlTypeUtil.isDate(in) && expected == SqlTypeFamily.TIMESTAMP) {
      return factory.createSqlType(SqlTypeName.TIMESTAMP);
    }
    // Implicit cast between date and timestamp.
    if (SqlTypeUtil.isTimestamp(in) && expected == SqlTypeFamily.DATE) {
      return factory.createSqlType(SqlTypeName.DATE);
    }
    // If the function accepts any numeric type and the input is a string, we
    // return the expected type family default type.
    // REVIEW chenyuzhao 2018-05-22: same with SQL-SERVER and Spark.
    if (SqlTypeUtil.isCharacter(in) && numericFamilies.contains(expected)) {
      return expected.getDefaultConcreteType(factory);
    }
    // string + date -> date; string + time -> time; string + timestamp -> timestamp
    if (SqlTypeUtil.isCharacter(in) && dateTimeFamilies.contains(expected)) {
      return expected.getDefaultConcreteType(factory);
    }
    // string + binary -> varbinary
    if (SqlTypeUtil.isCharacter(in) && expected == SqlTypeFamily.BINARY) {
      return expected.getDefaultConcreteType(factory);
    }
    // if we get here, `in` will never be a string type.
    if (SqlTypeUtil.isAtomic(in)
        && (expected == SqlTypeFamily.STRING
        || expected == SqlTypeFamily.CHARACTER)) {
      return expected.getDefaultConcreteType(factory);
    }
    return null;
  }
}

// End AbstractTypeCoercion.java
