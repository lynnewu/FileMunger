using Microsoft.Win32;

using System.Collections.Generic;
using System.Runtime.Versioning;
using System.Text;
using System.Threading;

namespace FileMunger.modules {
	/// <summary>
	/// Static class to process files and retrieve Windows file associations
	/// </summary>
	[SupportedOSPlatform("windows")]
	public static class GetAssociations {
		// Using a regular Dictionary with a reader-writer lock for better performance
		private static readonly Dictionary<string, FileAssociationInfo> _extensionMap =
				new Dictionary<string, FileAssociationInfo>(StringComparer.OrdinalIgnoreCase);
		private static readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

		private static int _processedFilesCount = 0;
		private static int _uniqueExtensionsCount = 0;

		/// <summary>
		/// Gets the number of files that have been processed
		/// </summary>
		public static int ProcessedFilesCount => _processedFilesCount;

		/// <summary>
		/// Gets the number of unique file extensions found
		/// </summary>
		public static int UniqueExtensionsCount => _uniqueExtensionsCount;

		/// <summary>
		/// Gets all the file associations that have been found
		/// </summary>
		public static IReadOnlyDictionary<string, FileAssociationInfo> Extensions {
			get {
				_rwLock.EnterReadLock();
				try {
					// Create a copy to avoid issues with enumeration
					return new Dictionary<string, FileAssociationInfo>(_extensionMap, StringComparer.OrdinalIgnoreCase);
				}
				finally {
					_rwLock.ExitReadLock();
				}
			}
		}

		/// <summary>
		/// Processes a file to gather extension association information
		/// </summary>
		/// <param name="filePath">The path to the file to process</param>
		/// <returns>True if this was a new extension, false if it was already known</returns>
		public static bool ProcessFile(string filePath) {
			if (string.IsNullOrEmpty(filePath)) {
				return false;
			}

			try {
				// Count each file we try to process
				Interlocked.Increment(ref _processedFilesCount);

				// Get the extension (including the dot)
				string extension = Path.GetExtension(filePath);

				// Skip files with no extension
				if (string.IsNullOrEmpty(extension)) {
					return false;
				}

				// First check if we already have this extension (read lock only)
				_rwLock.EnterReadLock();
				try {
					if (_extensionMap.ContainsKey(extension)) {
						return false; // Already processed this extension
					}
				}
				finally {
					_rwLock.ExitReadLock();
				}

				// If we get here, we need to try to add the extension
				_rwLock.EnterUpgradeableReadLock();
				try {
					// Check again inside the upgradeable read lock
					if (_extensionMap.ContainsKey(extension)) {
						return false; // Someone else already added it
					}

					// Create the association info
					FileAssociationInfo info = GetFileAssociationInfo(extension);

					// Enter write lock to update the dictionary
					_rwLock.EnterWriteLock();
					try {
						_extensionMap[extension] = info;
						Interlocked.Increment(ref _uniqueExtensionsCount);
						return true;
					}
					finally {
						_rwLock.ExitWriteLock();
					}
				}
				finally {
					_rwLock.ExitUpgradeableReadLock();
				}
			}
			catch (Exception ex) {
				Logger.LogWarning($"Error processing file association for {filePath}: {ex.Message}");
				return false;
			}
		}

		/// <summary>
		/// Gets file association information for a given extension
		/// </summary>
		private static FileAssociationInfo GetFileAssociationInfo(string extension) {
			FileAssociationInfo info = new FileAssociationInfo {
				Extension = extension
			};

			try {
				// Get the file type (e.g., .txt -> txtfile)
				using (RegistryKey? extensionKey = Registry.ClassesRoot.OpenSubKey(extension)) {
					if (extensionKey != null) {
						info.FileType = extensionKey.GetValue(string.Empty) as string;

						// Get the content type/MIME type
						info.ContentType = extensionKey.GetValue("Content Type") as string;

						// Try to get the perceived type
						using (RegistryKey? perceivedKey = extensionKey.OpenSubKey("PerceivedType")) {
							if (perceivedKey != null) {
								info.PerceivedType = perceivedKey.GetValue(string.Empty) as string;
							}
						}
					}
				}

				// If we found a file type, look up its open command
				if (!string.IsNullOrEmpty(info.FileType)) {
					using (RegistryKey? typeKey = Registry.ClassesRoot.OpenSubKey(info.FileType)) {
						if (typeKey != null) {
							// Try to get the friendly name
							info.FriendlyTypeName = typeKey.GetValue(string.Empty) as string;

							// Look for the open command
							using (RegistryKey? shellKey = typeKey.OpenSubKey("shell")) {
								if (shellKey != null) {
									// First try the default command
									string defaultCommand = shellKey.GetValue(string.Empty) as string ?? "open";

									using (RegistryKey? openKey = shellKey.OpenSubKey(defaultCommand)) {
										if (openKey != null) {
											using (RegistryKey? commandKey = openKey.OpenSubKey("command")) {
												if (commandKey != null) {
													info.OpenCommand = commandKey.GetValue(string.Empty) as string;
												}
											}
										}
									}
								}
							}
						}
					}
				}

				// If we still don't have an open command, try the UserChoice
				if (string.IsNullOrEmpty(info.OpenCommand)) {
					using (RegistryKey? userChoiceKey = Registry.CurrentUser.OpenSubKey(
							$"Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\FileExts\\{extension}\\UserChoice")) {
						if (userChoiceKey != null) {
							string? progId = userChoiceKey.GetValue("ProgId") as string;
							if (!string.IsNullOrEmpty(progId)) {
								info.FileType = progId;

								using (RegistryKey? typeKey = Registry.ClassesRoot.OpenSubKey(progId)) {
									if (typeKey != null) {
										info.FriendlyTypeName = typeKey.GetValue(string.Empty) as string;

										using (RegistryKey? shellKey = typeKey.OpenSubKey("shell\\open\\command")) {
											if (shellKey != null) {
												info.OpenCommand = shellKey.GetValue(string.Empty) as string;
											}
										}
									}
								}
							}
						}
					}
				}
			}
			catch (Exception ex) {
				Logger.LogWarning($"Error retrieving file association for {extension}: {ex.Message}");
			}

			return info;
		}

		/// <summary>
		/// Prints the results of file association gathering in a well-formatted table
		/// </summary>
		/// <returns>A string containing the formatted table of results</returns>
		public static string PrintResultsTable() {
			// Take a read lock to safely get a snapshot of the data
			_rwLock.EnterReadLock();
			List<FileAssociationInfo> associations;
			try {
				associations = new List<FileAssociationInfo>(_extensionMap.Values);
			}
			finally {
				_rwLock.ExitReadLock();
			}

			// Sort the associations by extension
			associations.Sort((a, b) => string.Compare(a.Extension, b.Extension, StringComparison.OrdinalIgnoreCase));

			// Calculate column widths for nice formatting
			int extensionWidth = Math.Max(10, associations.Max(a => a.Extension?.Length ?? 0) + 2);
			int typeWidth = Math.Max(10, associations.Max(a => a.FileType?.Length ?? 0) + 2);
			int nameWidth = Math.Max(20, Math.Min(30, associations.Max(a => a.FriendlyTypeName?.Length ?? 0) + 2));
			int mimeWidth = Math.Max(15, Math.Min(25, associations.Max(a => a.ContentType?.Length ?? 0) + 2));
			int perceivedWidth = Math.Max(12, associations.Max(a => a.PerceivedType?.Length ?? 0) + 2);
			int commandWidth = Math.Max(30, Math.Min(60, associations.Max(a => a.OpenCommand?.Length ?? 0) + 2));

			// Build the table
			StringBuilder sb = new StringBuilder();

			// Add header
			sb.AppendLine($"File Association Results: {_uniqueExtensionsCount} unique extensions from {_processedFilesCount} processed files");
			sb.AppendLine();

			// Column headers
			sb.Append(PadRight("Extension", extensionWidth));
			sb.Append(PadRight("Type", typeWidth));
			sb.Append(PadRight("Friendly Name", nameWidth));
			sb.Append(PadRight("MIME Type", mimeWidth));
			sb.Append(PadRight("Perceived", perceivedWidth));
			sb.AppendLine("Open Command");

			// Separator line
			sb.Append(new string('-', extensionWidth));
			sb.Append(new string('-', typeWidth));
			sb.Append(new string('-', nameWidth));
			sb.Append(new string('-', mimeWidth));
			sb.Append(new string('-', perceivedWidth));
			sb.AppendLine(new string('-', commandWidth));

			// Table data
			foreach (FileAssociationInfo info in associations) {
				sb.Append(PadRight(info.Extension ?? string.Empty, extensionWidth));
				sb.Append(PadRight(info.FileType ?? string.Empty, typeWidth));
				sb.Append(PadRight(TruncateString(info.FriendlyTypeName ?? string.Empty, nameWidth - 2), nameWidth));
				sb.Append(PadRight(TruncateString(info.ContentType ?? string.Empty, mimeWidth - 2), mimeWidth));
				sb.Append(PadRight(info.PerceivedType ?? string.Empty, perceivedWidth));
				sb.AppendLine(TruncateString(info.OpenCommand ?? string.Empty, commandWidth - 2));
			}

			return sb.ToString();
		}

		/// <summary>
		/// Truncates a string if it exceeds the specified length and adds ellipsis
		/// </summary>
		private static string TruncateString(string text, int maxLength) {
			if (text.Length <= maxLength) {
				return text;
			}

			return text.Substring(0, maxLength - 3) + "...";
		}

		/// <summary>
		/// Pads a string on the right to the specified width
		/// </summary>
		private static string PadRight(string text, int width) {
			return text.PadRight(width);
		}

		/// <summary>
		/// Releases resources used by the GetAssociations class
		/// </summary>
		public static void Cleanup() {
			_rwLock.Dispose();
		}
	}

	/// <summary>
	/// Contains information about a file extension and its associated applications
	/// </summary>
	public class FileAssociationInfo {
		/// <summary>
		/// The file extension (including the dot)
		/// </summary>
		public string Extension { get; set; } = string.Empty;

		/// <summary>
		/// The registered file type for the extension
		/// </summary>
		public string? FileType { get; set; }

		/// <summary>
		/// The human-readable name of the file type
		/// </summary>
		public string? FriendlyTypeName { get; set; }

		/// <summary>
		/// The MIME content type for the extension
		/// </summary>
		public string? ContentType { get; set; }

		/// <summary>
		/// The perceived type of the file (e.g., video, text, image)
		/// </summary>
		public string? PerceivedType { get; set; }

		/// <summary>
		/// The command used to open files with this extension
		/// </summary>
		public string? OpenCommand { get; set; }

		/// <summary>
		/// Returns a string representation of the file association information
		/// </summary>
		public override string ToString() {
			return $"{Extension} ({FileType ?? "unknown type"}): {OpenCommand ?? "No open command"}";
		}
	}
}