library dorm_sembast;

import 'package:dorm/dorm_interface.dart';
import 'package:sembast/sembast.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:sembast_web/sembast_web.dart';

import 'package:dorm_sembast/src/SembastDatabase.dart';

class DormSembastDatabaseProvider extends IDormDatabaseProvider {
  Future<IDormDatabase> _initialize(Database sembastDb, int previousVersion, int currentVersion, IDormConfiguration configuration) async {
    final db = SembastDatabase(sembastDb);
    await configuration.applyTo(db);
    return db;
  }

  @override
  Future<IDormDatabase> openDatabase(String databaseName, IDormConfiguration configuration, { bool inMemory = false, bool reset = false }) async {
    DatabaseFactory factory; 
    if (inMemory) {
      factory = databaseFactoryMemory;
    } else {
      factory = databaseFactoryWeb;
    }
    late IDormDatabase db;
    await factory.openDatabase(databaseName, version: 0, onVersionChanged: (sembastDb, prevVer, curVer) async {
      db = await _initialize(sembastDb, prevVer, curVer, configuration);
    });
    return db;
  }
}
