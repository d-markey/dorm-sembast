import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';
import 'package:sembast/utils/value_utils.dart';

import 'SembastDatabase.dart';
import 'SembastQueryGenerator.dart';
import 'SembastStore.dart';
import 'KeyTracker.dart';

class SembastTransaction extends IDormTransaction {
  SembastTransaction(this._db, this._transaction);

  final SembastDatabase _db;
  final Transaction _transaction;
  final _keyTracker = KeyTracker();

  @override
  IDormDatabase get db => _db;

  static final _completed = Future.value();

  @override
  Future createTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormField<T>> columns, Iterable<IDormIndex<T>>? indexes }) => _completed;

  @override
  Future addColumns<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormField<T>> columns, Iterable<IDormIndex<T>>? indexes }) => _completed;

  @override
  Future addIndexes<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormIndex<T>>? indexes }) => _completed;

  @override
  Future deleteTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model) {
    final store = _db.removeStore(model.entityName);
    return (store == null) ? _completed : store.drop(_db.database);
  }

  @override
  Future renameTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model, String name, String newName) => _db.renameStore<K>(name, newName);

  @override
  Future deleteIndexes<K, T extends IDormEntity<K>>(IDormModel<K, T> model, Iterable<IDormIndex<T>> indexesNames) => _completed;

  @override
  Future deleteColumns<K, T extends IDormEntity<K>>(IDormModel<K, T> model, Iterable<IDormField<T>> columnNames) => _completed;

  @override
  Future<int> dbCount<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) async {
    final model = _db.getModel<K, T>();
    final store = SembastStore(_db.getStore<K>(model.entityName)!);
    final selectQuery = QueryGenerator.getSelectQuery(model, whereClause);
    var count = await store.count(_transaction, filter: selectQuery.filter);
    if (selectQuery.pageOption.startAt != null) {
      count -= selectQuery.pageOption.startAt!;
      if (count < 0) count = 0;
    }
    if (selectQuery.pageOption.limit != null && count > selectQuery.pageOption.limit!) {
      count = selectQuery.pageOption.limit!;
    }
    return count;
  }

  @override
  Future<bool> dbAny<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) async {
    final count = await dbCount(whereClause);
    return count > 0;
  }

  @override
  Future<Iterable<K>> dbLoadKeys<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) async {
    final model = _db.getModel<K, T>();
    final store = SembastStore(_db.getStore<K>(model.entityName)!);
    final selectQuery = QueryGenerator.getSelectQuery(model, whereClause);
    return await store.loadKeys(_transaction, finder: selectQuery.finder);
  }

  @override
  bool isTracked(Type entityType, dynamic key) => _keyTracker.isTracked(entityType, key);

  @override
  Future<Iterable<DormRecord>> dbLoad<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) async {
    final model = _db.getModel<K, T>();
    final store = SembastStore(_db.getStore<K>(model.entityName)!);
    final selectQuery = QueryGenerator.getSelectQuery(model, whereClause);
    final rs = await store.load(_transaction, finder: selectQuery.finder);
    return rs.map((e) {
      final map = cloneMap(e.value);
      map[model.key.name] = e.key;
      _keyTracker.track(T, e.key);
      return map;
    });
  }

  @override
  Future<K> dbUpsert<K, T extends IDormEntity<K>>(DormRecord item) async {
    final model = _db.getModel<K, T>();
    K? key;
    if (K == int) {
      final store = SembastStore(_db.getStore<int>(model.entityName)!);
      key = item[model.key.name];
      if (key == null) {
        // INSERT
        key = await store.add(_transaction, item) as K;
        item[model.key.name] = key;
        await store.update(_transaction, item, finder: Finder(filter: Filter.byKey(key)));
      } else {
        // UPDATE
        await store.update(_transaction, item, finder: Finder(filter: Filter.byKey(key)));
      }
    } else {
      // UPSERT
      final store = SembastStore(_db.getStore<K>(model.entityName)!);
      key = item[model.key.name];
      await store.update(_transaction, item, finder: Finder(filter: Filter.byKey(key)));
    }
    return key!;
  }

  @override
  Future dbDelete<K, T extends IDormEntity<K>>(IDormClause whereClause) async {
    final model = _db.getModel<K, T>();
    final store = SembastStore(_db.getStore<K>(model.entityName)!);
    final deleteQuery = QueryGenerator.getDeleteQuery(model, whereClause);
    try {
      await store.delete(_transaction, finder: deleteQuery.finder);
    } on Exception catch (ex) {
      throw DormException('Sembast delete failed', inner: ex);
    }
  }

  void dispose() {
    _keyTracker.dispose();
  }
}
