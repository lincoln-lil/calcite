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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of Calcite implicit type cast.
 */
public class TypeCoercionImpl extends AbstractTypeCoercion {

  public TypeCoercionImpl(SqlValidator validator) {
    super(validator);
  }

  /**
   * Widen a SqlNode ith column type to target type, mainly used for set operations like UNION,
   * INTERSECT, EXCEPT.
   * <p>Rules:</p>
   * <pre>
   *
   *       type1, type2  type3       select a, b, c from t1
   *         |      |      |                 union
   *         |      |      |
   *       type4  type5  type6       select d, e, f from t2
   *         |      |      |
   *         |      |      |
   *       type7  type8  type9
   * </pre>
   * If we union struct type [type1, type2, type3] and type [type4, type5, type6] together, we would
   * decide the first result column type type7 from finding wider type for type1 and type4,
   * the second column type from type2 and type5 and so forth.
   *
   * @param scope       validator scope
   * @param query       SqlNode which have children nodes as columns
   * @param columnIndex target column index
   * @param targetType  target type to cast to
   * @return true if we add any cast in successfully.
   */
  public boolean widenColumnTypes(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType) {
    final SqlKind kind = query.getKind();
    switch (kind) {
    case SELECT:
      SqlSelect selectNode = (SqlSelect) query;
      SqlValidatorScope scope1 = validator.getSelectScope(selectNode);
      if (!coerceIthColumnTo(scope1, selectNode.getSelectList(), columnIndex, targetType)) {
        return false;
      }
      updateInferredRowType(scope1, query, columnIndex, targetType);
      return true;
    case VALUES:
      for (SqlNode rowConstructor : ((SqlCall) query).getOperandList()) {
        if (!coerceIthOperandTo(scope, (SqlCall) rowConstructor, columnIndex, targetType)) {
          return false;
        }
      }
      updateInferredRowType(scope, query, columnIndex, targetType);
      return true;
    case WITH:
      SqlNode body = ((SqlWith) query).body;
      return widenColumnTypes(scope, body, columnIndex, targetType);
    case UNION:
    case INTERSECT:
    case EXCEPT:
      // setops are binary for now.
      return widenColumnTypes(scope, ((SqlCall) query).operand(0), columnIndex, targetType)
          && widenColumnTypes(scope, ((SqlCall) query).operand(1), columnIndex, targetType);
    default:
      return false;
    }
  }

  /**
   * Coerce operands in binary arithmetic expressions to Numeric types.
   * <p>Rules:</p>
   * <ul>
   *   <li>For arithmetic operators like: + - * / %: 1. If operand is varchar, we cast it to
   *   double type; 2. If is DIVIDE operator, we promote all operands to
   *   double type.</li>
   *   <li>For operator =: 1. If operands are boolean and numeric, we make 1=true and 0=false all
   *   be true; 2. If operands are datetime and string, we do nothing cause Calcite already make
   *   the type coercion.</li>
   *   <li>For binary comparision, = > >= < <=: try to find the common type, i.e. 1 > '1' will
   *   be cast to 1>1</li>
   *   <li>some single agg func transform, all will coerce string type to double.</li>
   * </ul>
   */
  public boolean binaryArithmeticPromote(SqlCallBinding binding) {
    // We assert that the operand has a NUMERIC_NUMERIC/NUMERIC check type.
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    boolean changed = false;
    // binary operator
    if (binding.getOperandCount() == 2) {
      RelDataType type1 = binding.getOperandType(0);
      RelDataType type2 = binding.getOperandType(1);
      //special case for datetime +/- interval
      if (kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
        if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isInterval(type2)) {
          return false;
        }
      }
      // binary operator like: + - * / %
      if (kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
        changed = binaryArithmeticWithStrings(binding);
        changed = divisionToFractional(binding, type1, type2) || changed;
      }
      // = <> operator
      if (kind.belongsTo(SqlKind.BINARY_EQUALITY)) {
        //string <-> datetime
        //boolean <-> numeric | boolean <-> literal
        changed = dateTimeStringEquality(binding, type1, type2) || changed;
        changed = booleanEquality(binding, type1, type2) || changed;
      }
      // operator like: = > >= < <=
      if (kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
        RelDataType commonType = commonTypeForBinaryComparison(type1, type2);
        if (null != commonType) {
          changed = coerceIthOperandTo(binding.getScope(), binding.getCall(), 0, commonType)
              || changed;
          changed = coerceIthOperandTo(binding.getScope(), binding.getCall(), 1, commonType)
              || changed;
        }
      }
    }
    // single operand agg func cast, all string -> double.
    if (binding.getOperandCount() == 1) {
      RelDataType type = validator.deriveType(binding.getScope(), binding.operand(0));
      boolean isCharacterType = SqlTypeUtil.isCharacter(type);
      if (operator.getName().equalsIgnoreCase("ABS")
          && isCharacterType) {
        return coerceIthOperandTo(binding.getScope(), binding.getCall(), 0,
            factory.createSqlType(SqlTypeName.DOUBLE));
      }
      // There are not necessary here, we just enum
      switch (kind) {
      case SUM:
      case SUM0:
      case AVG:
      case STDDEV_POP:
      case STDDEV_SAMP:
      case MINUS_PREFIX:
      case PLUS_PREFIX:
      case VAR_POP:
      case VAR_SAMP:
        if (isCharacterType) {
          return coerceIthOperandTo(binding.getScope(), binding.getCall(), 0,
              factory.createSqlType(SqlTypeName.DOUBLE));
        }
      }
    }
    return changed;
  }

  /**
   * We always cast / operands to fractional types, integral division should use
   * {@link SqlStdOperatorTable#DIVIDE_INTEGER} operator.
   */
  protected boolean divisionToFractional(
      SqlCallBinding binding,
      RelDataType left,
      RelDataType right) {
    if (binding.getOperator().getKind() != SqlKind.DIVIDE) {
      return false;
    }
    boolean changed = false;
    // Do nothing if already a double or decimal
    if (isDoubleOrDecimal(left) || isDoubleOrDecimal(right)) {
      return false;
    }

    if (isNumericOrNull(left)) {
      RelDataType targetType = factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.DOUBLE),
          left.isNullable());
      changed = coerceIthOperandTo(binding.getScope(), binding.getCall(), 0, targetType)
          || changed;
    }
    if (isNumericOrNull(right)) {
      RelDataType targetType = factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.DOUBLE),
          right.isNullable());
      changed = coerceIthOperandTo(binding.getScope(), binding.getCall(), 1, targetType)
          || changed;
    }
    return changed;
  }

  private static boolean isDoubleOrDecimal(RelDataType type) {
    return SqlTypeUtil.isDouble(type) || SqlTypeUtil.isDecimal(type);
  }

  private static boolean isNumericOrNull(RelDataType type) {
    return SqlTypeUtil.isNumeric(type) || SqlTypeUtil.isNull(type);
  }

  /**
   * For numeric + string, we cast string to double.
   **/
  protected boolean binaryArithmeticWithStrings(SqlCallBinding binding) {
    boolean changed = false;
    for (int i = 0; i < binding.getOperandCount(); i++) {
      RelDataType type = binding.getOperandType(i);
      if (SqlTypeUtil.isCharacter(type)) {
        // promote all the string type operand to decimal type.
        boolean tmp = coerceIthOperandTo(binding.getScope(), binding.getCall(), i,
            factory.createSqlType(SqlTypeName.DOUBLE));
        changed = tmp || changed;
      }
    }
    return changed;
  }

  /**
   * Datetime and string equality: we will cast string type to datetime type, Calcite already did
   * it but we still keep this interface overridable so user can have their custom implementation.
   */
  protected boolean dateTimeStringEquality(
      SqlCallBinding binding,
      RelDataType left,
      RelDataType right) {
    // REVIEW 2018-05-23 chenyuzhao we do not need to do it now cause Calcite already did it in sql
    // to rel conversion, except for <> operator.
    if (binding.getCall().getKind() == SqlKind.NOT_EQUALS) {
      if (SqlTypeUtil.isCharacter(left)
          && SqlTypeUtil.isDatetime(right)) {
        return coerceIthOperandTo(binding.getScope(), binding.getCall(), 0, right);
      }
      if (SqlTypeUtil.isCharacter(right)
          && SqlTypeUtil.isDatetime(left)) {
        return coerceIthOperandTo(binding.getScope(), binding.getCall(), 1, left);
      }
    }
    return false;
  }

  /**
   * Cast boolean = numeric to numeric = numeric. For numeric literals like 1=`expr` or
   * 0=`expr`, we can cut it to `expr` and `not expr`, but these things should be done in planner
   * Rules. There are 2 cases that need type coercion here:
   * <ol>
   *   <li>Case1: `boolean expr1` = 1 or `boolean expr1` = 0, we replace the numeric literal with
   *   `true` or `false` boolean literal.</li>
   *   <li>Case2: `boolean expr1` = `numeric expr2`, we replace expr1 to `1` or `0` numeric
   *   literal.</li>
   * </ol>
   * For case2, we just add in cast node here, during sql to rel converting calcite will replace
   * `cast(expr1 as right)` with `case when expr1 then 1 else 0.`
   */
  protected boolean booleanEquality(SqlCallBinding binding, RelDataType left, RelDataType right) {
    SqlNode lNode = binding.operand(0);
    SqlNode rNode = binding.operand(1);
    if (SqlTypeUtil.isNumeric(left)
        && SqlTypeUtil.isBoolean(right)) {
      // case1: literal numeric + boolean
      if (lNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((SqlLiteral) lNode).bigDecimalValue();
        if (val.compareTo(BigDecimal.ONE) == 0) {
          SqlNode lNode1 = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
          binding.getCall().setOperand(0, lNode1);
          return true;
        } else {
          SqlNode lNode1 = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
          binding.getCall().setOperand(0, lNode1);
          return true;
        }
      }
      // case2: boolean + numeric
      return coerceIthOperandTo(binding.getScope(), binding.getCall(), 1, left);
    }

    if (SqlTypeUtil.isNumeric(right)
        && SqlTypeUtil.isBoolean(left)) {
      // case1: literal numeric + boolean
      if (rNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((SqlLiteral) rNode).bigDecimalValue();
        if (val.compareTo(new BigDecimal(1)) == 0) {
          SqlNode rNode1 = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
          binding.getCall().setOperand(1, rNode1);
          return true;
        } else {
          SqlNode rNode1 = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
          binding.getCall().setOperand(1, rNode1);
          return true;
        }
      }
      // case2: boolean + numeric
      return coerceIthOperandTo(binding.getScope(), binding.getCall(), 0, right);
    }
    return false;
  }


  /**
   * Case when and COALESCE type coercion, we collect all the branches types including then
   * operands and else operands to find a common type, then cast the operands to desired type if
   * needed.
   */
  public boolean caseWhenCoercion(SqlCallBinding callBinding) {
    // for clause like `case when ... then (a, b, c) when ... then (d, e, f) else (g, h, i)`
    // an exception would be thrown before this.
    SqlCase caseCall = (SqlCase) callBinding.getCall();
    SqlNodeList thenList = caseCall.getThenOperands();
    List<RelDataType> argTypes = new ArrayList<RelDataType>();
    for (SqlNode node : thenList) {
      argTypes.add(
          validator.deriveType(
              callBinding.getScope(), node));
    }
    SqlNode elseOp = caseCall.getElseOperand();
    RelDataType elseOpType = validator.deriveType(
        callBinding.getScope(), caseCall.getElseOperand());
    argTypes.add(elseOpType);
    // We have got a wider type when we entered this method, recompute it here
    // just to make the method calling clean.
    RelDataType widerType = getWiderTypeFor(argTypes, true);
    if (null != widerType) {
      boolean changed = false;
      for (int i = 0; i < thenList.size(); i++) {
        changed = coerceIthColumnTo(callBinding.getScope(), thenList, i, widerType) || changed;
      }
      if (needToCast(callBinding.getScope(), elseOp, widerType)) {
        changed = coerceIthOperandTo(callBinding.getScope(), caseCall, 3, widerType)
            || changed;
      }
      return changed;
    }
    return false;
  }

  /**
   * STRATEGIES:
   * <p>For with/without subquery:</p>
   * <ul>
   * <li>With subquery: find the common type by comparing the left hand side (LHS)
   * expression types against corresponding right hand side (RHS) expression derived
   * from the subquery expression's plan output. Inject appropriate casts in the
   * LHS and RHS side of IN expression.</li>
   * <li>Without subquery: convert the value and in the RHS to the
   * common operator type by looking at all the argument types and finding
   * the closest one that all the arguments can be cast to.</li>
   * </ul>
   * <p>For finding common data types of struct types:
   * <pre>
   *
   *          LHS type         |          RHS type
   *
   *    field1  --- field2          field3  --- field4
   *      |           |                |           |
   *      |           +------type2-----+-----------+
   *      |                            |
   *      +-------------type1----------+
   *
   *
   *
   * <field1, field2, field3>    <field4, field5, field6>
   *    |        |       |          |       |       |
   *    +--------+---type1----------+       |       |
   *             |       |                  |       |
   *             +-------+----type2---------+       |
   *                     |                          |
   *                     +-------------type3--------+
   * </pre>
   * <p>For both basic sql types(LHS and RHS):
   * Cast all the nodes in RHS to desired common type if needed.</p>
   * <p>For both struct sql types:
   * Cast every fields to the desired type for both LHS and RHS.</p>
   */
  public boolean inOperationConversion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    if (operator.getKind() == SqlKind.IN) {
      assert binding.getOperandCount() == 2;
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      final SqlNode node1 = binding.operand(0);
      final SqlNode node2 = binding.operand(1);
      final SqlValidatorScope scope = binding.getScope();
      if (type1.isStruct()
          && type2.isStruct()
          && type1.getFieldCount() != type2.getFieldCount()) {
        return false;
      }
      int colCount = type1.isStruct() ? type1.getFieldCount() : 1;
      final RelDataType[] argTypes = new RelDataType[2];
      argTypes[0] = type1;
      argTypes[1] = type2;
      boolean changed = false;
      List<RelDataType> widenTypes = new ArrayList<>();
      for (int i = 0; i < colCount; i++) {
        final int i2 = i;
        List<RelDataType> columnIthTypes = new AbstractList<RelDataType>() {
          public RelDataType get(int index) {
            return argTypes[index].isStruct()
                ? argTypes[index].getFieldList().get(i2).getType()
                : argTypes[index];
          }

          public int size() {
            return argTypes.length;
          }
        };

        RelDataType widenType = commonTypeForBinaryComparison(columnIthTypes.get(0),
            columnIthTypes.get(1));
        if (widenType == null) {
          widenType = getTightestCommonType(columnIthTypes.get(0), columnIthTypes.get(1));
        }
        if (widenType == null) {
          // If we can not find any common type, just return early.
          return false;
        }
        widenTypes.add(widenType);
      }
      // We find all the common type for RSH and LSH columns.
      assert widenTypes.size() == colCount;
      for (int i = 0; i < widenTypes.size(); i++) {
        RelDataType desired = widenTypes.get(i);
        // LSH maybe a row values or single node.
        if (node1.getKind() == SqlKind.ROW) {
          assert node1 instanceof SqlCall;
          if (coerceIthOperandTo(scope, (SqlCall) node1, i, desired)) {
            updateInferredRowType(scope, node1, i, widenTypes.get(i));
            changed = true;
          }
        } else {
          changed = coerceIthOperandTo(scope, binding.getCall(), 0, desired)
              || changed;
        }
        // RSH may be a values rows or subquery.
        if (node2 instanceof SqlNodeList) {
          final SqlNodeList node3 = (SqlNodeList) node2;
          boolean listChanged = false;
          if (type2.isStruct()) {
            for (SqlNode node : (SqlNodeList) node2) {
              assert node instanceof SqlCall;
              listChanged = coerceIthOperandTo(scope, (SqlCall) node, i, desired) || listChanged;
            }
            if (listChanged) {
              updateInferredRowType(scope, node2, i, desired);
            }
          } else {
            for (int j = 0; j < ((SqlNodeList) node2).size(); j++) {
              listChanged = coerceIthColumnTo(scope, node3, j, desired) || listChanged;
            }
            if (listChanged) {
              updateInferredTypesFor(node2, desired);
            }
          }
          changed = listChanged || changed;
        } else {
          // another subquery.
          SqlValidatorScope scope1 = node2 instanceof SqlSelect
              ? validator.getSelectScope((SqlSelect) node2)
              : scope;
          changed = widenColumnTypes(scope1, node2, i, desired) || changed;
        }
      }
      return changed;
    }
    return false;
  }

  public boolean implicitTypeCast(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies) {
    assert binding.getOperandCount() == operandTypes.size();
    if (!canImplicitTypeCast(operandTypes, expectedFamilies)) {
      return false;
    }
    boolean changed = false;
    for (int i = 0; i < operandTypes.size(); i++) {
      RelDataType implicitType = implicitCast(operandTypes.get(i), expectedFamilies.get(i));
      changed = null != implicitType
          && operandTypes.get(i) != implicitType
          && coerceIthOperandTo(binding.getScope(), binding.getCall(), i, implicitType)
          || changed;
    }
    return changed;
  }

  /**
   * Type coercion for non builtin functions.
   */
  public boolean coerceFunctionParams(SqlValidatorScope scope, SqlCall call, SqlFunction function) {
    final List<RelDataType> paramTypes = function.getParamTypes();
    assert paramTypes != null;
    boolean changed = false;
    // user defined table macro can only allows literals.
    // we should support this later on. JDBC Test
    // TableFunctionTest.testUserDefinedTableFunction.
    if (function instanceof SqlUserDefinedTableMacro) {
      return false;
    }
    for (int i = 0; i < call.operandCount(); i++) {
      SqlNode operand = call.operand(i);
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();
        String name = ((SqlIdentifier) operandList.get(1)).getSimple();
        int formalIndex = function.getParamNames().indexOf(name);
        if (formalIndex < 0) {
          return false;
        }
        // We do not support column list operand type now.
        changed = coerceIthOperandTo(scope, (SqlCall) operand, 0,
            paramTypes.get(formalIndex)) || changed;
      } else {
        changed = coerceIthOperandTo(scope, call, i, paramTypes.get(i)) || changed;
      }
    }
    return changed;
  }
}

// End TypeCoercionImpl.java
