import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';

import 'SembastOperand.dart';

import 'extensions.dart';

class SembastFieldOperand extends SembastOperand {
  SembastFieldOperand(this.fieldName) : super((RecordSnapshot record) => record[fieldName]);

  final String fieldName;

  @override
  Filter getComparisonFilter(IDormComparisonExpression expr) {
    switch (expr.op) {
      case DormExpressionOperator.IsNull:
        return Filter.isNull(fieldName);
      case DormExpressionOperator.IsNotNull:
        return Filter.notNull(fieldName);
      case DormExpressionOperator.Equals:
        return Filter.equals(fieldName, expr.value);
      case DormExpressionOperator.IsNotEqual:
        return Filter.notEquals(fieldName, expr.value);
      case DormExpressionOperator.LessThan:
        return Filter.lessThan(fieldName, expr.value);
      case DormExpressionOperator.LessOrEqual:
        return Filter.lessThanOrEquals(fieldName, expr.value);
      case DormExpressionOperator.MoreThan:
        return Filter.greaterThan(fieldName, expr.value);
      case DormExpressionOperator.MoreOrEqual:
        return Filter.greaterThanOrEquals(fieldName, expr.value);
      case DormExpressionOperator.Contains:
        return Filter.custom((record) => getValue(record)?.contains(expr.value) ?? false);
      case DormExpressionOperator.StartsWith:
        return Filter.custom((record) => getValue(record)?.startsWith(expr.value) ?? false);
      case DormExpressionOperator.EndsWith:
        return Filter.custom((record) => getValue(record)?.endsWith(expr.value) ?? false);
      case DormExpressionOperator.InList:
        final values = expr.value as Iterable?;
        if (values == null || values.isEmpty) {
          return Filter.custom((rs) => false);
        } else {
          return Filter.inList(fieldName, values.asList());
        }
      case DormExpressionOperator.NotInList:
        final values = expr.value as Iterable?;
        if (values == null || values.isEmpty) {
          return Filter.custom((rs) => true);
        } else {
          return Filter.not(Filter.inList(fieldName, values.asList()));
        }
      default: throw DormException('Unsupported clause $expr');
    }
  }

  @override
  Filter getRangeFilter(IDormRangeExpression expr) {
    switch (expr.op) {
      case DormExpressionOperator.InRange:
        if (expr.min == null && expr.max == null) {
          return Filter.custom((record) => true);
        } else if (expr.min == null) {
          return Filter.lessThanOrEquals(fieldName, expr.max);
        } else if (expr.max == null) {
          return Filter.greaterThanOrEquals(fieldName, expr.min);
        } else {
          return Filter.and([ Filter.greaterThanOrEquals(fieldName, expr.min), Filter.lessThanOrEquals(fieldName, expr.max) ]);
        }
      case DormExpressionOperator.NotInRange:
        if (expr.min == null && expr.max == null) {
          return Filter.custom((record) => false);
        } else if (expr.min == null) {
          return Filter.greaterThan(fieldName, expr.max);
        } else if (expr.max == null) {
          return Filter.lessThan(fieldName, expr.min);
        } else {
          return Filter.or([ Filter.lessThan(fieldName, expr.min), Filter.greaterThan(fieldName, expr.max) ]);
        }
      default: throw DormException('Unsupported clause $expr');
    }
  }
}

