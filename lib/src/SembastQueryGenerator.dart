import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';

import 'SembastFieldOperand.dart';
import 'SembastOperand.dart';

class PageOption {
  int? limit;
  int? startAt;
}

class Query {
  String? _store;
  String get store => _store!;

  String? _keyName;
  String get keyName => _keyName!;

  DormRecord? _item;
  DormRecord get item => _item!;

  Finder? _finder;
  Finder get finder => _finder!;

  Filter? _filter;
  Filter get filter => _filter!;

  PageOption? _pageOption;
  PageOption get pageOption => _pageOption!;
}

Filter _buildPageFilter(IDormPageClause expr, PageOption pageOption) {
  if (expr is IDormLimitClause) {
    pageOption.limit ??= expr.max;
    return _buildFilter(expr.clause, pageOption);
  }
  if (expr is IDormOffsetClause) {
    pageOption.startAt ??= expr.startAt;
    return _buildFilter(expr.clause, pageOption);
  }
  throw DormException('Unsupported clause $expr');
}

Filter _buildBinaryFilter(IDormBinaryExpression expr, PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.And: return Filter.and([ _buildFilter(expr.left, pageOption), _buildFilter(expr.right, pageOption) ]);
    case DormExpressionOperator.Or: return Filter.or([ _buildFilter(expr.left, pageOption), _buildFilter(expr.right, pageOption) ]);
    default: throw DormException('Unsupported clause $expr');
  }
}

Filter _buildUnaryFilter(IDormUnaryExpression expr, PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.Not: return Filter.not(_buildFilter(expr.expression, pageOption));
    default: throw DormException('Unsupported clause $expr');
  }
}

Filter _buildZeroaryFilter(IDormZeroaryExpression expr, PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.All: return Filter.custom((record) => true);
    case DormExpressionOperator.None: return Filter.custom((record) => false);
    default: throw DormException('Unsupported clause $expr');
  }
}

SembastOperand _buildOperandExpression(IDormOperandExpression expr) {
  if (expr is IDormColumnExpression) {
    return SembastFieldOperand(expr.column.name);
  } else {
    final operand = _buildOperandExpression(expr.operand!);
    switch (expr.op) {
      case DormExpressionOperator.ToLower:
        return SembastOperand((record) => operand.getValue(record)?.toLowerCase());
      case DormExpressionOperator.Trim:
        return SembastOperand((record) => operand.getValue(record)?.trim());
      case DormExpressionOperator.Length:
        return SembastOperand((record) => operand.getValue(record)?.length());
      default:
        throw DormException('Unsupported clause $expr');
    }
  }
}

Filter _buildRangeFilter(IDormRangeExpression expr) {
  final operand = _buildOperandExpression(expr.operand);
  return operand.getRangeFilter(expr);
}

Filter _buildComparisonFilter(IDormComparisonExpression expr) {
  final operand = _buildOperandExpression(expr.operand);
  return operand.getComparisonFilter(expr);
}

Filter _buildFilter(IDormClause expr, PageOption pageOption) {
  if (expr is IDormPageClause) return _buildPageFilter(expr, pageOption);
  if (expr is IDormComparisonExpression) return _buildComparisonFilter(expr);
  if (expr is IDormRangeExpression) return _buildRangeFilter(expr);
  if (expr is IDormZeroaryExpression) return _buildZeroaryFilter(expr, pageOption);
  if (expr is IDormUnaryExpression) return _buildUnaryFilter(expr, pageOption);
  if (expr is IDormBinaryExpression) return _buildBinaryFilter(expr, pageOption);
  throw DormException('Unsupported clause $expr');
}

class _SembastClause {
  _SembastClause(this.filter, this.pageOption) : finder = Finder(filter: filter);

  final Finder finder;
  final Filter filter;
  final PageOption pageOption;
}

_SembastClause _buildFinder(IDormClause expr) {
  final pageOption = PageOption();
  final clause = _SembastClause(_buildFilter(expr, pageOption), pageOption);
  if (pageOption.limit != null) clause.finder.limit = pageOption.limit!;
  if (pageOption.startAt != null) clause.finder.offset = pageOption.startAt!;
  return clause;
}

class QueryGenerator {
  static Query getSelectQuery(IDormModel model, IDormClause? whereClause) {
    final select = Query();
    select._store = model.entityName;
    select._keyName = model.key.name;
    final clause = _buildFinder(whereClause ?? all());
    select._filter = clause.filter;
    select._finder = clause.finder;
    return select;
  }

  static Query getInsertQuery(IDormModel model, DormRecord item) {
    final insert = Query();
    insert._store = model.entityName;
    insert._keyName = model.key.name;
    insert._item = item;
    return insert;
  }

  static Query getUpdateQuery(IDormModel model, DormRecord item) {
    final update = Query();
    update._store = model.entityName;
    update._keyName = model.key.name;
    update._item = item;
    return update;
  }

  static Query getUpsertQuery(IDormModel model, DormRecord item) {
    final upsert = Query();
    upsert._store = model.entityName;
    upsert._keyName = model.key.name;
    upsert._item = item;
    return upsert;
  }

  static Query getDeleteQuery(IDormModel model, IDormClause? whereClause) {
    final delete = Query();
    delete._store = model.entityName;
    delete._keyName = model.key.name;
    final clause = _buildFinder(whereClause ?? all());
    delete._finder = clause.finder;
    delete._filter = clause.filter;
    return delete;
  }
}