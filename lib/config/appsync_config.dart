/// AppSync configuration constants.
///
/// Replace [apiUrl] with your actual AWS AppSync endpoint.
/// The [region] must match the region where the AppSync API is deployed.
///
/// These values can be found in the AWS AppSync console under
/// "Settings" for your API.
class AppSyncConfig {
  AppSyncConfig._();

  /// The HTTPS endpoint for the AppSync GraphQL API.
  /// Example: 'https://xxxxxxxxxxxxxxxxxxxxxxxxxx.appsync-api.us-east-1.amazonaws.com/graphql'
  static const String apiUrl = String.fromEnvironment(
    'APPSYNC_API_URL',
    defaultValue: 'https://REPLACE_WITH_YOUR_APPSYNC_ENDPOINT/graphql',
  );

  /// AWS region where the AppSync API is hosted.
  static const String region = String.fromEnvironment(
    'AWS_REGION',
    defaultValue: 'us-east-1',
  );
}
