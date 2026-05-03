/// Represents a single item returned by the `listMinaretItems` GraphQL query.
class MinaretItem {
  final String mountName;
  final String title;
  final String category;
  final String website;

  const MinaretItem({
    required this.mountName,
    required this.title,
    required this.category,
    required this.website,
  });

  /// Creates a [MinaretItem] from a GraphQL JSON response object.
  factory MinaretItem.fromJson(Map<String, dynamic> json) {
    return MinaretItem(
      mountName: (json['mount_name'] as String?) ?? '',
      title: (json['title'] as String?) ?? '',
      category: (json['category'] as String?) ?? '',
      website: (json['website'] as String?) ?? '',
    );
  }

  @override
  String toString() =>
      'MinaretItem(mountName: $mountName, title: $title, '
      'category: $category, website: $website)';
}
