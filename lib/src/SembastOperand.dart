import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';

class SembastOperand {
  SembastOperand(this._getter);

  final dynamic Function(RecordSnapshot record) _getter;

  dynamic getValue(RecordSnapshot record) => _getter(record);

  Filter getComparisonFilter(IDormComparisonExpression expr) {
    switch (expr.op) {
      case DormExpressionOperator.IsNull:
        return Filter.custom((record) => getValue(record) == null);
      case DormExpressionOperator.IsNotNull:
        return Filter.custom((record) => getValue(record) != null);
      case DormExpressionOperator.Equals:
        return Filter.custom((record) => getValue(record) == expr.value);
      case DormExpressionOperator.IsNotEqual:
        return Filter.custom((record) => getValue(record) != expr.value);
      case DormExpressionOperator.LessThan:
        return Filter.custom((record) => getValue(record) < expr.value);
      case DormExpressionOperator.LessOrEqual:
        return Filter.custom((record) => getValue(record) <= expr.value);
      case DormExpressionOperator.MoreThan:
        return Filter.custom((record) => getValue(record) > expr.value);
      case DormExpressionOperator.MoreOrEqual:
        return Filter.custom((record) => getValue(record) >= expr.value);
      case DormExpressionOperator.Contains:
        return Filter.custom((record) => getValue(record)?.contains(expr.value) ?? false);
      case DormExpressionOperator.StartsWith:
        return Filter.custom((record) => getValue(record)?.startsWith(expr.value) ?? false);
      case DormExpressionOperator.EndsWith:
        return Filter.custom((record) => getValue(record)?.endsWith(expr.value) ?? false);
      case DormExpressionOperator.InList:
        var values = expr.value as Iterable?;
        if (values == null || values.isEmpty) {
          return Filter.custom((rs) => false);
        } else {
          return Filter.custom((record) {
            final recordValue = getValue(record);
            return values.any((v) => recordValue == v);
          });
        }
      case DormExpressionOperator.NotInList:
        final values = expr.value as Iterable?;
        if (values == null || values.isEmpty) {
          return Filter.custom((rs) => true);
        } else {
          return Filter.custom((record) {
            final recordValue = getValue(record);
            return values.every((v) => recordValue != v);
          });
        }
      default: throw DormException('Unsupported clause $expr');
    }
  }

  Filter getRangeFilter(IDormRangeExpression expr) {
    switch (expr.op) {
      case DormExpressionOperator.InRange:
        if (expr.min == null && expr.max == null) {
          return Filter.custom((record) => true);
        } else if (expr.min == null) {
          return Filter.custom((record) => getValue(record) <= expr.max);
        } else if (expr.max == null) {
          return Filter.custom((record) => getValue(record) >= expr.min);
        } else {
          return Filter.custom((record) {
            final value = getValue(record);
            return (expr.min <= value) && (value <= expr.max);
          });
        }
      case DormExpressionOperator.NotInRange:
        if (expr.min == null && expr.max == null) {
          return Filter.custom((record) => false);
        } else if (expr.min == null) {
          return Filter.custom((record) => getValue(record) > expr.max);
        } else if (expr.max == null) {
          return Filter.custom((record) => getValue(record) < expr.min);
        } else {
          return Filter.custom((record) {
            final value = getValue(record);
            return (value < expr.min) || (value > expr.max);
          });
        }
      default: throw DormException('Unsupported clause $expr');
    }
  }
}
