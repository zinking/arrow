// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <charconv>
#include <unordered_map>
#include <string_view>
#include <type_traits>

#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"
#include "arrow/compute/expression.h"
#include "rapidjson/document.h"


namespace arrow {
namespace compute {
constexpr char kOp[] = "op";
constexpr char kName[] = "name";
constexpr char kValue[] = "value";
constexpr char kChild[] = "child";
constexpr char kLeft[] = "left";
constexpr char kRight[] = "right";
constexpr char kKlass[] = "class";
constexpr char kVal[] = "val";

enum class Operation {
  TRUE,
  FALSE,
  NOT,
  AND,
  OR,
  IS_NULL,
  NOT_NULL,
  IS_NAN,
  NOT_NAN,
  LT,
  LT_EQ,
  GT,
  GT_EQ,
  EQ,
  NOT_EQ,
  STARTS_WITH,
  NOT_STARTS_WITH,
  IN,
  NOT_IN
};

std::string SafeGetStrVal(const char* key, const rapidjson::Value& value) {
  auto it = value.FindMember(key);
  if (it != value.MemberEnd() && it->value.IsString()) {
    return it->value.GetString();
  }
  return "";
}

Operation GetOperation(const char* key, const rapidjson::Value& value) {
  std::string op_str = SafeGetStrVal(key, value);
  if (op_str == "TRUE") return Operation::TRUE;
  if (op_str == "TRUE") return Operation::TRUE;
  if (op_str == "FALSE") return Operation::FALSE;
  if (op_str == "NOT") return Operation::NOT;
  if (op_str == "AND") return Operation::AND;
  if (op_str == "OR") return Operation::OR;
  if (op_str == "IS_NULL") return Operation::IS_NULL;
  if (op_str == "NOT_NULL") return Operation::NOT_NULL;
  if (op_str == "IS_NAN") return Operation::IS_NAN;
  if (op_str == "NOT_NAN") return Operation::NOT_NAN;
  if (op_str == "LT") return Operation::LT;
  if (op_str == "LT_EQ") return Operation::LT_EQ;
  if (op_str == "GT") return Operation::GT;
  if (op_str == "GT_EQ") return Operation::GT_EQ;
  if (op_str == "EQ") return Operation::EQ;
  if (op_str == "NOT_EQ") return Operation::NOT_EQ;
  if (op_str == "STARTS_WITH") return Operation::STARTS_WITH;
  if (op_str == "NOT_STARTS_WITH") return Operation::NOT_STARTS_WITH;
  if (op_str == "IN") return Operation::IN;
  if (op_str == "NOT_IN") return Operation::NOT_IN;
  throw std::runtime_error("Unknown operation: " + op_str);
}

Datum LiteralFromJson(const rapidjson::Value& json_node) {
  ARROW_CHECK(json_node.IsObject())
    << "Cannot parse expression from non-object: " << json_node.GetString();

  std::string klass = SafeGetStrVal(kKlass, json_node);
  if (klass == "IntegerLiteral") {
    int32_t int_val = json_node[kVal].GetInt();
    return Datum(int_val);
  } else if (klass == "LongLiteral") {
    int64_t long_val = json_node[kVal].GetInt64();
    return Datum(long_val);
  } else if (klass == "BooleanLiteral") {
    bool bool_val = json_node[kVal].GetBool();
    return Datum(bool_val);
  } else if (klass == "FloatLiteral") {
    float float_val = static_cast<float>(json_node[kVal].GetDouble());
    return Datum(float_val);
  } else if (klass == "DoubleLiteral") {
    double double_val = json_node[kVal].GetDouble();
    return Datum(double_val);
  } else if (klass == "DateLiteral") {
    int32_t date_val = json_node[kVal].GetInt();
    return Datum(date_val);
  } else if (klass == "TimeLiteral") {
    int64_t time_val = json_node[kVal].GetInt64();
    return Datum(time_val);
  } else if (klass == "DecimalLiteral") {
    std::string decimal_val_str = json_node[kVal].GetString();
    // Implement conversion from string to Decimal128
    throw std::runtime_error("error convert json literal Decimal128");
  } else if (klass == "StringLiteral") {
    std::string str_val = json_node[kVal].GetString();
    return Datum(str_val);
  } else if (klass == "UUIDLiteral") {
    std::string uuid_val_str = json_node[kVal].GetString();
    return Datum(uuid_val_str);
  } else if (klass == "FixedLiteral") {
    std::string fixed_val_str = json_node[kVal].GetString();
    throw std::runtime_error("error convert json literal FixedLiteral");
  } else if (klass == "BinaryLiteral") {
    std::string binary_val_str = json_node[kVal].GetString();
    throw std::runtime_error("error convert json literal BinaryLiteral");
  } else {
    throw std::runtime_error("error convert json literal");
  }
}

std::vector<Datum> VisitLiterals(const rapidjson::Value& node) {
  std::vector<Datum> literals;
  if (node.IsArray()) {
    for (const auto& literal_node : node.GetArray()) {
      literals.push_back(LiteralFromJson(literal_node));
    }
  }
  return literals;
}

Datum VisitLiteral(const rapidjson::Value& node) {
  std::vector<Datum> literals = VisitLiterals(node);
  if (literals.size() == 0) {
    throw std::runtime_error("error convert literals, 0 literals found");
  }
  return literals[0];
}

Expression VisitLiteralExpr(const rapidjson::Value& node) {
  const rapidjson::Value &litNode = node.GetObject()[kValue];
  return arrow::compute::literal(VisitLiteral(litNode));
}

Expression Visit(const rapidjson::Value& json) {
  Operation op = GetOperation(kOp, json);
  switch (op) {
    case Operation::TRUE:
      return arrow::compute::literal(true);
    case Operation::FALSE:
      return arrow::compute::literal(false);
    case Operation::NOT: {
      const rapidjson::Value &child = json.GetObject()[kChild];
      auto node = Visit(child);
      return arrow::compute::not_(node);
    }
    case Operation::AND: {
      const rapidjson::Value &left = json.GetObject()[kLeft];
      const rapidjson::Value &right = json.GetObject()[kRight];
      auto lhs = Visit(left);
      auto rhs = Visit(right);
      return arrow::compute::and_(lhs, rhs);
    }
    case Operation::OR: {
      const rapidjson::Value &left = json.GetObject()[kLeft];
      const rapidjson::Value &right = json.GetObject()[kRight];
      auto lhs = Visit(left);
      auto rhs = Visit(right);
      return arrow::compute::or_(lhs, rhs);
    }
    case Operation::IS_NULL: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      return arrow::compute::is_valid(field);
    }
    case Operation::NOT_NULL: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      return arrow::compute:: not_(arrow::compute::is_valid(field));
    }
    case Operation::IS_NAN: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      return arrow::compute::is_null(field, true);
    }
    case Operation::NOT_NAN: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      return arrow::compute:: not_(arrow::compute::is_null(field, true));
    }
    case Operation::LT: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::less(field, lit);
    }
    case Operation::LT_EQ: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::less_equal(field, lit);
    }
    case Operation::GT: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::greater(field, lit);
    }
    case Operation::GT_EQ: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::greater_equal(field, lit);
    }
    case Operation::EQ: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::equal(field, lit);
    }
    case Operation::NOT_EQ: {
      const rapidjson::Value &fieldName = json.GetObject()[kName];
      auto field = arrow::compute::field_ref(fieldName.GetString());
      auto lit = VisitLiteralExpr(json);
      return arrow::compute::not_equal(field, lit);
    }

    /* following to be supported
    case STARTS_WITH:
      Object literal1 = visitLiteral(jsonNode);
      String startsWith1 = Objects.isNull(literal1) ? VALUE_DEFAULT : literal1.toString();
      return Expressions.startsWith(JsonUtil.getString(NAME, jsonNode), startsWith1);
    case NOT_STARTS_WITH:
      Object literal0 = visitLiteral(jsonNode);
      String startsWith0 = Objects.isNull(literal0) ? VALUE_DEFAULT : literal0.toString();
      return Expressions.notStartsWith(JsonUtil.getString(NAME, jsonNode), startsWith0);
    case IN:
      Iterable<Object> invals = visitLiterals(jsonNode);
      return Expressions.in(JsonUtil.getString(NAME, jsonNode), invals);
    case NOT_IN:
      Iterable<Object> notInVals = visitLiterals(jsonNode);
      return Expressions.notIn(JsonUtil.getString(NAME, jsonNode), notInVals);
      */
    default:
      ARROW_LOG(FATAL) << "Cannot convert JSON to Expression: "
        << json.GetString() << static_cast<int>(op);
      throw std::runtime_error("error convert json string");
  }
}

Expression FromJson(const rapidjson::Value& json) {
  ARROW_CHECK(json.IsObject())
    << "Cannot parse expression from non-object: " << json.GetString();
  return Visit(json);
}

Result<Expression> Expression::FromString(std::string_view expr_str) {
  rapidjson::Document document;
  std::string jsonString(expr_str);
  if (document.Parse(jsonString.c_str()).HasParseError()) {
    return arrow::Status::Invalid("Error parsing JSON");
  } else {
    return FromJson(document.GetObject());
  }
}

}  // namespace compute
}  // namespace arrow
