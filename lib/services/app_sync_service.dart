import 'dart:convert';

import 'package:http/http.dart' as http;

import '../config/appsync_config.dart';
import '../models/minaret_item.dart';

/// Service responsible for executing GraphQL queries against AWS AppSync.
///
/// Authentication is performed via the Cognito User Pool JWT token supplied
/// through [getIdToken].  No direct DynamoDB access is performed here.
class AppSyncService {
  /// Callback that returns the current Cognito User Pool ID token (JWT).
  ///
  /// Inject this from whichever auth service/state-management layer is
  /// already managing Cognito sessions in the application.
  final Future<String> Function() getIdToken;

  /// Optional HTTP client – useful for testing.
  final http.Client _client;

  AppSyncService({
    required this.getIdToken,
    http.Client? httpClient,
  }) : _client = httpClient ?? http.Client();

  static const String _listMinaretItemsQuery = r'''
    query ListMinaretItems {
      listMinaretItems {
        mount_name
        title
        category
        website
      }
    }
  ''';

  /// Fetches all items from the `listMinaretItems` query.
  ///
  /// Throws an [AppSyncException] on network errors or GraphQL errors.
  Future<List<MinaretItem>> listMinaretItems() async {
    final String idToken = await getIdToken();

    final response = await _client.post(
      Uri.parse(AppSyncConfig.apiUrl),
      headers: {
        'Content-Type': 'application/json',
        // AppSync accepts Cognito User Pool tokens via the Authorization header.
        'Authorization': idToken,
      },
      body: jsonEncode({
        'query': _listMinaretItemsQuery,
      }),
    );

    if (response.statusCode != 200) {
      throw AppSyncException(
        'HTTP ${response.statusCode}: ${response.reasonPhrase}',
      );
    }

    final Map<String, dynamic> body =
        jsonDecode(response.body) as Map<String, dynamic>;

    // GraphQL errors are returned with a 200 status but contain an "errors" key.
    if (body.containsKey('errors')) {
      final errors = body['errors'] as List<dynamic>;
      final messages = errors
          .map((e) => (e as Map<String, dynamic>)['message'] as String? ?? '')
          .join(', ');
      throw AppSyncException('GraphQL error(s): $messages');
    }

    final data = body['data'] as Map<String, dynamic>?;
    final items = data?['listMinaretItems'] as List<dynamic>?;

    if (items == null) {
      return [];
    }

    return items
        .cast<Map<String, dynamic>>()
        .map(MinaretItem.fromJson)
        .toList();
  }

  /// Closes the underlying HTTP client.
  void dispose() => _client.close();
}

/// Exception thrown when an AppSync request fails.
class AppSyncException implements Exception {
  final String message;
  const AppSyncException(this.message);

  @override
  String toString() => 'AppSyncException: $message';
}
