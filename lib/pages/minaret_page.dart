import 'package:flutter/material.dart';

import '../models/minaret_item.dart';
import '../services/app_sync_service.dart';

/// Displays a paginated list of minaret items fetched from AWS AppSync.
///
/// The page preserves the same visual layout (DataTable with fixed columns)
/// that was previously backed by a direct DynamoDB Scan.
class MinaretPage extends StatefulWidget {
  /// The AppSync service used to fetch data.
  ///
  /// Should be constructed once and passed in (e.g. from a DI container or
  /// provider) so the underlying HTTP client is not recreated on every rebuild.
  final AppSyncService appSyncService;

  const MinaretPage({super.key, required this.appSyncService});

  @override
  State<MinaretPage> createState() => _MinaretPageState();
}

class _MinaretPageState extends State<MinaretPage> {
  /// The column definitions shown in the table.
  ///
  /// Keys are the GraphQL field names; values are the display labels.
  /// Changing only this map is enough to add/remove/reorder columns.
  static const Map<String, String> _columns = {
    'mount_name': 'Mount',
    'title': 'Title',
    'category': 'Category',
    'website': 'Website',
  };

  List<MinaretItem> _items = [];
  bool _isLoading = false;
  String? _errorMessage;

  @override
  void initState() {
    super.initState();
    _fetchItems();
  }

  Future<void> _fetchItems() async {
    setState(() {
      _isLoading = true;
      _errorMessage = null;
    });

    try {
      final items = await widget.appSyncService.listMinaretItems();
      if (mounted) {
        setState(() {
          _items = items;
          _isLoading = false;
        });
      }
    } on AppSyncException catch (e) {
      if (mounted) {
        setState(() {
          _errorMessage = e.message;
          _isLoading = false;
        });
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _errorMessage = 'An unexpected error occurred. Please try again.';
          _isLoading = false;
        });
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Build helpers
  // ---------------------------------------------------------------------------

  Widget _buildLoadingState() {
    return const Center(child: CircularProgressIndicator());
  }

  Widget _buildErrorState() {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          const Icon(Icons.error_outline, color: Colors.red, size: 48),
          const SizedBox(height: 12),
          Text(
            _errorMessage ?? 'Unknown error',
            textAlign: TextAlign.center,
            style: const TextStyle(color: Colors.red),
          ),
          const SizedBox(height: 16),
          ElevatedButton.icon(
            onPressed: _fetchItems,
            icon: const Icon(Icons.refresh),
            label: const Text('Retry'),
          ),
        ],
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          const Text('No items found.'),
          const SizedBox(height: 16),
          ElevatedButton.icon(
            onPressed: _fetchItems,
            icon: const Icon(Icons.refresh),
            label: const Text('Refresh'),
          ),
        ],
      ),
    );
  }

  Widget _buildTable() {
    // Wrap in a single scrollable area: vertical via ListView, horizontal via
    // SingleChildScrollView so the DataTable can grow in both axes without
    // nested-scroll conflicts.
    return SingleChildScrollView(
      scrollDirection: Axis.vertical,
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: DataTable(
          columns: _columns.values
              .map((label) => DataColumn(label: Text(label)))
              .toList(),
          rows: _items.map(_buildRow).toList(),
        ),
      ),
    );
  }

  DataRow _buildRow(MinaretItem item) {
    // Map each column key to the corresponding field on the model.
    final Map<String, String> fieldValues = {
      'mount_name': item.mountName,
      'title': item.title,
      'category': item.category,
      'website': item.website,
    };

    return DataRow(
      cells: _columns.keys
          .map((key) => DataCell(Text(fieldValues[key] ?? '')))
          .toList(),
    );
  }

  // ---------------------------------------------------------------------------
  // Build
  // ---------------------------------------------------------------------------

  @override
  Widget build(BuildContext context) {
    Widget body;

    if (_isLoading) {
      body = _buildLoadingState();
    } else if (_errorMessage != null) {
      body = _buildErrorState();
    } else if (_items.isEmpty) {
      body = _buildEmptyState();
    } else {
      body = _buildTable();
    }

    return Scaffold(
      appBar: AppBar(
        title: const Text('Minaret'),
        actions: [
          IconButton(
            tooltip: 'Refresh',
            icon: const Icon(Icons.refresh),
            onPressed: _isLoading ? null : _fetchItems,
          ),
        ],
      ),
      body: body,
    );
  }
}
