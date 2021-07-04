import 'dart:async';

import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';

import 'extensions.dart';
import 'SembastTransaction.dart';

class SembastDatabase extends DormDatabase {
  SembastDatabase(this.database);

  final Database database;

  @override
  final defaultKeyName = '_id';

  int? _dbToInt(dynamic value) => value as int?;
  bool? _dbToBool(dynamic value) => (value == null) ? null : (_dbToInt(value) == 0 ? false : true);
  num? _dbToNum(dynamic value) => value as num?;
  String? _dbToString(dynamic value) => value as String?;
  DateTime? _dbToDateTime(dynamic value) => (value == null) ? null : DateTime.fromMillisecondsSinceEpoch(1000 * _dbToInt(value)!, isUtc: true);
  List<int>? _dbToBlob(dynamic value) => (value as List).map((e) => e as int).asList();

  @override
  T? castFromDb<T>(dynamic value) {
    if (value == null) return null;
    if (isType<T, int>()) return _dbToInt(value) as T?;
    if (isType<T, bool>()) return _dbToBool(value) as T?;
    if (isType<T, num>()) return  _dbToNum(value) as T?;
    if (isType<T, DateTime>()) return _dbToDateTime(value) as T?;
    if (isType<T, String>()) return _dbToString(value) as T?;
    if (isList<T, int>()) return _dbToBlob(value) as T?;
    throw DormException('unexpected type $T for $value');
  }

  dynamic intToDb(int? value) => value;
  dynamic boolToDb(bool? value) => (value == null) ? null : (value ? 1 : 0);
  dynamic numToDb(num? value) => value;
  dynamic stringToDb(String? value) => value;
  dynamic dateTimeToDb(DateTime? value) => (value == null) ? null : (value.toUtc().millisecondsSinceEpoch ~/ 1000);
  dynamic blobToDb(Iterable<int>? value) => value!.asList();

  @override
  dynamic castToDb(dynamic value) {
    if (value == null) return null;
    if (value is int) return intToDb(value);
    if (value is bool) return boolToDb(value);
    if (value is num) return  numToDb(value);
    if (value is DateTime) return dateTimeToDb(value);
    if (value is String) return stringToDb(value);
    if (value is List<int>) return blobToDb(value);
    throw DormException('unexpected type ${value.runtimeType} for $value');
  }

  final Map<String, StoreRef> _stores = <String, StoreRef>{};

  @override
  void registerModel<K, T extends IDormEntity<K>>(IDormModel<K, T> model) {
    if (!_stores.containsKey(model.entityName)) {
      final storeFactory = isType<K, int>() ? intMapStoreFactory : stringMapStoreFactory;
      _stores[model.entityName] = storeFactory.store(model.entityName);
    }
    super.registerModel(model);
  }

  StoreRef<K, DormRecord>? getStore<K>(String tableName) => _stores[tableName] as StoreRef<K, DormRecord>?;
  StoreRef<K, DormRecord>? removeStore<K>(String tableName) => _stores.remove(tableName) as StoreRef<K, DormRecord>?;

  Future renameStore<K>(String tableName, String newTableName) async {
    final storeFactory = isType<K, int>() ? intMapStoreFactory : stringMapStoreFactory;
    final from = storeFactory.store(tableName);
    final to = storeFactory.store(newTableName);
    await database.transaction((transaction) async {
      final items = await from.find(transaction);
      for (var item in items) {
        await to.add(transaction, item.value);
      }
      await from.drop(transaction);
    });
  }

  @override
  Future<T> transaction<T>(DormWorker<T> work) {
    return database.transaction((transaction) async {
      final trans = SembastTransaction(this, transaction);
      try {
        final res = await work(trans);
        return res;
      } finally {
        trans.dispose();
      }
    });
  }

  @override
  Future<T> readonly<T>(DormWorker<T> work) {
    return transaction(work);
  }

  @override
  void dispose() {
    database.close();
  }
}
