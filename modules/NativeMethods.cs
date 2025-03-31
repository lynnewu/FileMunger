
using FileMunger;
using System.Runtime.InteropServices;
using System.Text;

/// <summary>
/// Native Windows API methods for handling symbolic links
/// </summary>
internal static class NativeMethods {
	[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
	private static extern Int32 GetFinalPathNameByHandle(IntPtr handle, [MarshalAs(UnmanagedType.LPWStr)] StringBuilder path, Int32 pathLength, Int32 flags);

	[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
	private static extern IntPtr CreateFile(
			String lpFileName,
			UInt32 dwDesiredAccess,
			UInt32 dwShareMode,
			IntPtr lpSecurityAttributes,
			UInt32 dwCreationDisposition,
			UInt32 dwFlagsAndAttributes,
			IntPtr hTemplateFile);

	[DllImport("kernel32.dll", SetLastError = true)]
	[return: MarshalAs(UnmanagedType.Bool)]
	private static extern Boolean CloseHandle(IntPtr hObject);

	[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
	private static extern Int32 FormatMessage(
			UInt32 dwFlags,
			IntPtr lpSource,
			Int32 dwMessageId,
			Int32 dwLanguageId,
			StringBuilder lpBuffer,
			Int32 nSize,
			IntPtr Arguments);

	// Windows-specific constants
	private const UInt32 FILE_READ_ATTRIBUTES = 0x80;
	private const UInt32 FILE_WRITE_ATTRIBUTES = 0x0100;
	private const UInt32 FILE_SHARE_READ = 0x1;
	private const UInt32 FILE_SHARE_WRITE = 0x2;
	private const UInt32 OPEN_EXISTING = 3;
	private const UInt32 FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
	private const Int32 FILE_NAME_NORMALIZED = 0x0;
	private const UInt32 FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;
	private const UInt32 FORMAT_MESSAGE_IGNORE_INSERTS = 0x00000200;

	// Add these constants to the NativeMethods class
	private const UInt32 FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;
	private const UInt32 FILE_FLAG_SEQUENTIAL_SCAN = 0x08000000;
	private const UInt32 FILE_SHARE_DELETE = 0x00000004;
	/// <summary>
	/// Gets a readable description for a Win32 error code
	/// </summary>
	private static String GetWin32ErrorMessage(Int32 errorCode) {
		try {
			StringBuilder message = new StringBuilder(1024);
			Int32 size = FormatMessage(
					FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
					IntPtr.Zero,
					errorCode,
					0, // Default language
					message,
					message.Capacity,
					IntPtr.Zero);

			if (size > 0) {
				return message.ToString().Trim();
			}
		}
		catch (Exception) {
			// Fall back to just the error code if we can't get the message
		}

		return $"Unknown error {errorCode}";
	}


	/// <summary>
	/// Gets the final path name for a file, resolving any symbolic links.
	/// This method only ever opens files for read access.
	/// </summary>
	/// <param name="path">The path to resolve</param>
	/// <returns>The resolved final path, or null if resolution fails</returns>
	public static String? GetFinalPathName(String path) {
		Logger.LogDebug($"Resolving path with Windows API (read-only): {path}");

		IntPtr handle = IntPtr.Zero;

		try {
			// Always use minimum required read-only access
			UInt32 desiredAccess = FILE_READ_ATTRIBUTES;

			// Use maximum share mode for best compatibility with locked files
			UInt32 shareMode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

			// First try: Use backup semantics for directories + maximum share mode
			UInt32 flagsAndAttributes = FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT;

			// Open a handle to the file or directory - read-only
			handle = CreateFile(
					path,
					desiredAccess,
					shareMode,
					IntPtr.Zero,
					OPEN_EXISTING,
					flagsAndAttributes,
					IntPtr.Zero);

			if (handle == new IntPtr(-1)) {
				Int32 error = Marshal.GetLastWin32Error();

				// Second try: Use sequential scan hint which can help with certain locked files
				handle = CreateFile(
						path,
						desiredAccess,
						shareMode,
						IntPtr.Zero,
						OPEN_EXISTING,
						FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_SEQUENTIAL_SCAN,
						IntPtr.Zero);

				if (handle == new IntPtr(-1)) {
					error = Marshal.GetLastWin32Error();
					String errorMessage = GetWin32ErrorMessage(error);
					Logger.LogWarning($"Failed to open '{path}' for path resolution. Error {error}: {errorMessage}");
					return null;
				}
			}

			// If we get here, we have a valid handle
			StringBuilder result = new StringBuilder(512);
			Int32 length = GetFinalPathNameByHandle(handle, result, result.Capacity, FILE_NAME_NORMALIZED);

			if (length >= result.Capacity) {
				// Need a larger buffer
				result.EnsureCapacity(length + 1);
				length = GetFinalPathNameByHandle(handle, result, result.Capacity, FILE_NAME_NORMALIZED);
			}

			if (length > 0) {
				String finalPath = result.ToString();
				// Remove \\?\ prefix if present
				if (finalPath.StartsWith("\\\\?\\")) {
					finalPath = finalPath.Substring(4);
				}
				return finalPath;
			}

			Int32 finalError = Marshal.GetLastWin32Error();
			String finalErrorMessage = GetWin32ErrorMessage(finalError);
			Logger.LogWarning($"GetFinalPathNameByHandle failed for '{path}'. Error {finalError}: {finalErrorMessage}");
			return null;
		}
		catch (Exception ex) {
			Logger.LogWarning($"Exception resolving path '{path}': {ex.GetType().Name}: {ex.Message}");
			return null;
		}
		finally {
			// Always close the handle if it was opened successfully
			if (handle != IntPtr.Zero && handle != new IntPtr(-1)) {
				CloseHandle(handle);
			}
		}
	}

	// Remove the readOnly parameter from any call sites
	// Change: ResolveSymbolicLink(path, readOnly)
	// To: ResolveSymbolicLink(path)

}

