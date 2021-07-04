
import 'package:dorm/dorm.dart';
import 'package:sembast/sembast.dart';

class SembastStore<K> {
  SembastStore(this._store);

  final StoreRef<K, DormRecord> _store;

  String get name => _store.name;

  Future<int> count(Transaction transaction, { Filter? filter }) async {
    try {
      return await _store.count(transaction, filter: filter);
    } on Exception catch (ex) {
      throw DormException('Sembast count failed', inner: ex);
    }
  }

  Future<bool> any(Transaction transaction, { Filter? filter }) async {
    try {
      final finder = Finder(filter: filter);
      return (await _store.findFirst(transaction, finder: finder)) != null;
    } on Exception catch (ex) {
      throw DormException('Sembast count failed', inner: ex);
    }
  }

  Future<List<K>> loadKeys(Transaction transaction, { Finder? finder }) async {
    try {
      return await _store.findKeys(transaction, finder: finder);
    } on Exception catch (ex) {
      throw DormException('Sembast findKeys failed', inner: ex);
    }
  }

  Future<List<RecordSnapshot<K, DormRecord>>> load(Transaction transaction, { Finder? finder }) async {
    try {
      return await _store.query(finder: finder).getSnapshots(transaction);
    } on Exception catch (ex) {
      throw DormException('Sembast query failed', inner: ex);
    }
  }

  Future<K> add(Transaction transaction, DormRecord value) async {
    try {
      return await _store.add(transaction, value);
    } on Exception catch (ex) {
      throw DormException('Sembast add failed', inner: ex);
    }
  }

  Future<int> update(Transaction transaction, DormRecord item, { Finder? finder }) async {
    try {
      return await _store.update(transaction, item, finder: finder);
    } on Exception catch (ex) {
      throw DormException('Sembast update failed', inner: ex);
    }
  }

  Future<int> delete(Transaction transaction, { Finder? finder }) async {
    try {
      return await _store.delete(transaction, finder: finder);
    } on Exception catch (ex) {
      throw DormException('Sembast update failed', inner: ex);
    }
  }
}
