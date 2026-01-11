import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'package:ffi/ffi.dart';
import 'package:fpdart/fpdart.dart';
import 'package:hiddify/core/model/directories.dart';
import 'package:hiddify/gen/singbox_generated_bindings.dart';
import 'package:hiddify/singbox/model/singbox_config_option.dart';
import 'package:hiddify/singbox/model/singbox_outbound.dart';
import 'package:hiddify/singbox/model/singbox_stats.dart';
import 'package:hiddify/singbox/model/singbox_status.dart';
import 'package:hiddify/singbox/model/warp_account.dart';
import 'package:hiddify/singbox/service/singbox_service.dart';
import 'package:hiddify/utils/utils.dart';
import 'package:loggy/loggy.dart';
import 'package:path/path.dart' as p;
import 'package:rxdart/rxdart.dart';
import 'package:watcher/watcher.dart';

final _logger = Loggy('FFISingboxService');

/// Helper to get lib path - must be static/top-level for Isolate use
String _getLibPath() {
  String fullPath = "";
  if (Platform.environment.containsKey('FLUTTER_TEST')) {
    fullPath = "libcore";
  }
  if (Platform.isWindows) {
    fullPath = p.join(fullPath, "libcore.dll");
  } else if (Platform.isMacOS) {
    fullPath = p.join(fullPath, "libcore.dylib");
  } else {
    fullPath = p.join(fullPath, "libcore.so");
  }
  return fullPath;
}

/// Load library in current isolate
SingboxNativeLibrary _loadLibrary() {
  final path = _getLibPath();
  final lib = DynamicLibrary.open(path);
  return SingboxNativeLibrary(lib);
}

// ===== Isolate execution helpers =====
// These functions run in separate isolates and reload the library each time

/// Setup singbox in isolate
String _isolateSetup(List<dynamic> args) {
  final baseDir = args[0] as String;
  final workingDir = args[1] as String;
  final tempDir = args[2] as String;
  final port = args[3] as int;
  final debug = args[4] as bool;

  // Reload library in this isolate
  final box = _loadLibrary();

  // Initialize Dart API DL in this isolate as well, just in case
  box.setupOnce(NativeApi.initializeApiDLData);

  return box
      .setup(
        baseDir.toNativeUtf8().cast(),
        workingDir.toNativeUtf8().cast(),
        tempDir.toNativeUtf8().cast(),
        port,
        debug ? 1 : 0,
      )
      .cast<Utf8>()
      .toDartString();
}

/// Start singbox in isolate
String _isolateStart(List<dynamic> args) {
  final configPath = args[0] as String;
  final disableMemoryLimit = args[1] as bool;
  final box = _loadLibrary();
  return box
      .start(
        configPath.toNativeUtf8().cast(),
        disableMemoryLimit ? 1 : 0,
      )
      .cast<Utf8>()
      .toDartString();
}

/// Stop singbox in isolate
String _isolateStop(dynamic _) {
  final box = _loadLibrary();
  return box.stop().cast<Utf8>().toDartString();
}

/// Restart singbox in isolate
String _isolateRestart(List<dynamic> args) {
  final configPath = args[0] as String;
  final disableMemoryLimit = args[1] as bool;
  final box = _loadLibrary();
  return box
      .restart(
        configPath.toNativeUtf8().cast(),
        disableMemoryLimit ? 1 : 0,
      )
      .cast<Utf8>()
      .toDartString();
}

/// Change options in isolate
String _isolateChangeOptions(String optionsJson) {
  final box = _loadLibrary();
  return box.changeHiddifyOptions(optionsJson.toNativeUtf8().cast()).cast<Utf8>().toDartString();
}

/// Select outbound in isolate
String _isolateSelectOutbound(List<String> args) {
  final groupTag = args[0];
  final outboundTag = args[1];
  final box = _loadLibrary();
  return box
      .selectOutbound(
        groupTag.toNativeUtf8().cast(),
        outboundTag.toNativeUtf8().cast(),
      )
      .cast<Utf8>()
      .toDartString();
}

/// URL test in isolate
String _isolateUrlTest(String groupTag) {
  final box = _loadLibrary();
  return box.urlTest(groupTag.toNativeUtf8().cast()).cast<Utf8>().toDartString();
}

/// Validate config in isolate
String _isolateValidateConfig(List<dynamic> args) {
  final path = args[0] as String;
  final tempPath = args[1] as String;
  final debug = args[2] as bool;
  final box = _loadLibrary();
  return box
      .parse(
        path.toNativeUtf8().cast(),
        tempPath.toNativeUtf8().cast(),
        debug ? 1 : 0,
      )
      .cast<Utf8>()
      .toDartString();
}

/// Generate config in isolate
String _isolateGenerateConfig(String path) {
  final box = _loadLibrary();
  return box
      .generateConfig(
        path.toNativeUtf8().cast(),
      )
      .cast<Utf8>()
      .toDartString();
}

class FFISingboxService with InfraLogger implements SingboxService {
  static final SingboxNativeLibrary _box = _loadLibrary();

  late final ValueStream<SingboxStatus> _status;
  late final ReceivePort _statusReceiver;
  Stream<SingboxStats>? _serviceStatsStream;
  Stream<List<SingboxOutboundGroup>>? _outboundsStream;

  static SingboxNativeLibrary _gen() {
    _logger.debug('singbox native libs path: "${_getLibPath()}"');
    return _loadLibrary();
  }

  @override
  Future<void> init() async {
    loggy.debug("initializing");
    _statusReceiver = ReceivePort('service status receiver');
    final source = _statusReceiver.asBroadcastStream().map((event) => jsonDecode(event as String)).map(SingboxStatus.fromEvent);
    _status = ValueConnectableStream.seeded(
      source,
      const SingboxStopped(),
    ).autoConnect();

    loggy.debug("init completed (setup configured to run in isolate)");
  }

  @override
  TaskEither<String, Unit> setup(
    Directories directories,
    bool debug,
  ) {
    // Collect arguments to pass to the isolate
    final args = [
      directories.baseDir.path,
      directories.workingDir.path,
      directories.tempDir.path,
      _statusReceiver.sendPort.nativePort,
      debug,
    ];

    return TaskEither(
      () async {
        loggy.debug("before setup (in background isolate)");
        try {
          // Send only what's needed to a static helper to avoid capturing 'this'
          final err = await _runSetupInIsolate(args);

          loggy.debug("after setup, err: $err");
          if (err.isNotEmpty) {
            return left(err);
          }
          return right(unit);
        } catch (e, stack) {
          loggy.error("Error running setup in isolate", e, stack);
          return left(e.toString());
        }
      },
    );
  }

  // Static helper to ensure 'this' is not captured by Isolate.run closure
  static Future<String> _runSetupInIsolate(List<dynamic> args) {
    return Isolate.run(() => _isolateSetup(args));
  }

  @override
  TaskEither<String, Unit> validateConfigByPath(
    String path,
    String tempPath,
    bool debug,
  ) {
    return TaskEither(
      () async {
        try {
          final err = await Isolate.run(
            () => _isolateValidateConfig([path, tempPath, debug]),
          );
          if (err.isNotEmpty) {
            return left(err);
          }
          return right(unit);
        } catch (e) {
          return left(e.toString());
        }
      },
    );
  }

  @override
  TaskEither<String, Unit> changeOptions(SingboxConfigOption options) {
    return TaskEither(
      () async {
        final json = jsonEncode(options.toJson());
        final err = await Isolate.run(() => _isolateChangeOptions(json));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  @override
  TaskEither<String, String> generateFullConfigByPath(
    String path,
  ) {
    return TaskEither(
      () async {
        try {
          final response = await Isolate.run(
            () => _isolateGenerateConfig(path),
          );
          if (response.startsWith("error")) {
            return left(response.replaceFirst("error", ""));
          }
          return right(response);
        } catch (e) {
          return left(e.toString());
        }
      },
    );
  }

  @override
  TaskEither<String, Unit> start(
    String configPath,
    String name,
    bool disableMemoryLimit,
  ) {
    loggy.debug("starting, memory limit: [${!disableMemoryLimit}]");
    return TaskEither(
      () async {
        final err = await Isolate.run(() => _isolateStart([configPath, disableMemoryLimit]));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  @override
  TaskEither<String, Unit> stop() {
    return TaskEither(
      () async {
        final err = await Isolate.run(() => _isolateStop(null));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  @override
  TaskEither<String, Unit> restart(
    String configPath,
    String name,
    bool disableMemoryLimit,
  ) {
    loggy.debug("restarting, memory limit: [${!disableMemoryLimit}]");
    return TaskEither(
      () async {
        final err = await Isolate.run(() => _isolateRestart([configPath, disableMemoryLimit]));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  @override
  TaskEither<String, Unit> resetTunnel() {
    throw UnimplementedError(
      "reset tunnel function unavailable on platform",
    );
  }

  @override
  Stream<SingboxStatus> watchStatus() => _status;

  @override
  Stream<SingboxStats> watchStats() {
    if (_serviceStatsStream != null) return _serviceStatsStream!;
    final receiver = ReceivePort('stats');
    final statusStream = receiver.asBroadcastStream(
      onCancel: (_) {
        _logger.debug("stopping stats command client");
        final err = _box.stopCommandClient(1).cast<Utf8>().toDartString();
        if (err.isNotEmpty) {
          _logger.error("error stopping stats client");
        }
        receiver.close();
        _serviceStatsStream = null;
      },
    ).map(
      (event) {
        if (event case String _) {
          if (event.startsWith('error:')) {
            loggy.error("[service stats client] error received: $event");
            throw event.replaceFirst('error:', "");
          }
          return SingboxStats.fromJson(
            jsonDecode(event) as Map<String, dynamic>,
          );
        }
        loggy.error("[service status client] unexpected type, msg: $event");
        throw "invalid type";
      },
    );

    final err = _box.startCommandClient(1, receiver.sendPort.nativePort).cast<Utf8>().toDartString();
    if (err.isNotEmpty) {
      loggy.error("error starting status command: $err");
      throw err;
    }

    return _serviceStatsStream = statusStream;
  }

  @override
  Stream<List<SingboxOutboundGroup>> watchGroups() {
    final logger = newLoggy("watchGroups");
    if (_outboundsStream != null) return _outboundsStream!;
    final receiver = ReceivePort('groups');
    final outboundsStream = receiver.asBroadcastStream(
      onCancel: (_) {
        logger.debug("stopping");
        receiver.close();
        _outboundsStream = null;
        final err = _box.stopCommandClient(5).cast<Utf8>().toDartString();
        if (err.isNotEmpty) {
          _logger.error("error stopping group client");
        }
      },
    ).map(
      (event) {
        if (event case String _) {
          if (event.startsWith('error:')) {
            logger.error("error received: $event");
            throw event.replaceFirst('error:', "");
          }

          return (jsonDecode(event) as List).map((e) {
            return SingboxOutboundGroup.fromJson(e as Map<String, dynamic>);
          }).toList();
        }
        logger.error("unexpected type, msg: $event");
        throw "invalid type";
      },
    );

    try {
      final err = _box.startCommandClient(5, receiver.sendPort.nativePort).cast<Utf8>().toDartString();
      if (err.isNotEmpty) {
        logger.error("error starting group command: $err");
        throw err;
      }
    } catch (e) {
      receiver.close();
      rethrow;
    }

    return _outboundsStream = outboundsStream;
  }

  @override
  Stream<List<SingboxOutboundGroup>> watchActiveGroups() {
    final logger = newLoggy("[ActiveGroupsClient]");
    final receiver = ReceivePort('active groups');
    final outboundsStream = receiver.asBroadcastStream(
      onCancel: (_) {
        logger.debug("stopping");
        receiver.close();
        final err = _box.stopCommandClient(13).cast<Utf8>().toDartString();
        if (err.isNotEmpty) {
          logger.error("failed stopping: $err");
        }
      },
    ).map(
      (event) {
        if (event case String _) {
          if (event.startsWith('error:')) {
            logger.error(event);
            throw event.replaceFirst('error:', "");
          }

          return (jsonDecode(event) as List).map((e) {
            return SingboxOutboundGroup.fromJson(e as Map<String, dynamic>);
          }).toList();
        }
        logger.error("unexpected type, msg: $event");
        throw "invalid type";
      },
    );

    try {
      final err = _box.startCommandClient(13, receiver.sendPort.nativePort).cast<Utf8>().toDartString();
      if (err.isNotEmpty) {
        logger.error("error starting: $err");
        throw err;
      }
    } catch (e) {
      receiver.close();
      rethrow;
    }

    return outboundsStream;
  }

  @override
  TaskEither<String, Unit> selectOutbound(String groupTag, String outboundTag) {
    return TaskEither(
      () async {
        final err = await Isolate.run(() => _isolateSelectOutbound([groupTag, outboundTag]));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  @override
  TaskEither<String, Unit> urlTest(String groupTag) {
    return TaskEither(
      () async {
        final err = await Isolate.run(() => _isolateUrlTest(groupTag));
        if (err.isNotEmpty) {
          return left(err);
        }
        return right(unit);
      },
    );
  }

  final _logBuffer = <String>[];
  int _logFilePosition = 0;

  @override
  Stream<List<String>> watchLogs(String path) async* {
    yield await _readLogFile(File(path));
    yield* Watcher(path, pollingDelay: const Duration(seconds: 1)).events.asyncMap((event) async {
      if (event.type == ChangeType.MODIFY) {
        await _readLogFile(File(path));
      }
      return _logBuffer;
    });
  }

  @override
  TaskEither<String, Unit> clearLogs() {
    return TaskEither(
      () async {
        _logBuffer.clear();
        return right(unit);
      },
    );
  }

  Future<List<String>> _readLogFile(File file) async {
    if (_logFilePosition == 0 && file.lengthSync() == 0) return [];
    final content = await file.openRead(_logFilePosition).transform(utf8.decoder).join();
    _logFilePosition = file.lengthSync();
    final lines = const LineSplitter().convert(content);
    if (lines.length > 300) {
      lines.removeRange(0, lines.length - 300);
    }
    for (final line in lines) {
      _logBuffer.add(line);
      if (_logBuffer.length > 300) {
        _logBuffer.removeAt(0);
      }
    }
    return _logBuffer;
  }

  @override
  TaskEither<String, WarpResponse> generateWarpConfig({
    required String licenseKey,
    required String previousAccountId,
    required String previousAccessToken,
  }) {
    loggy.debug("generating warp config");
    return TaskEither(
      () async {
        final response = _box
            .generateWarpConfig(
              licenseKey.toNativeUtf8().cast(),
              previousAccountId.toNativeUtf8().cast(),
              previousAccessToken.toNativeUtf8().cast(),
            )
            .cast<Utf8>()
            .toDartString();
        if (response.startsWith("error:")) {
          return left(response.replaceFirst('error:', ""));
        }
        return right(warpFromJson(jsonDecode(response)));
      },
    );
  }
}
