# FileMunger

_**tl;dr:**_  kinda crappy <a href="https://en.wikipedia.org/wiki/Xargs" target="_blank">xargs</a>-ish <a href="https://en.wikipedia.org/wiki/Windows_Console" target="_blank">Windows console app</a> using .Net parallelism and thread-safety

FileMunger is a high-performance file processing Windows Console app designed to quickly traverse directories, handle symbolic links properly, and then perform some action on each file. 

It uses parallel processing optimized for both CPU and I/O performance.

Uses only open-for-read because it's kinda dumb to have a submachine gun with no safety.  OTOH, it was original written to do R/W, on the off-chance I wanted to seriously hose something important.  The changes for 

This is a conceptual offshoot of my <a href="https://github.com/lynnewu/LNKFileAnalyzer" target="_blank">LNKFileAnalyzer</a>

_**Note:**_  this code (including comments and README) was generated by the us.anthropic.claude-3-7-sonnet-20250219-v1:0 "AI" model under close supervision.

No attempts were made to do rational things like
  - make use of nuget packages, e.g. <a href="https://learn.microsoft.com/en-us/dotnet/standard/commandline/" target="_blank">Microsoft System.CommandLine</a>
  - it has not been thoroughly checked or tested for common "AI" sub-optimizing misfeaturettes such as off-by-one errors, pointless code, or just generally bad style.

##  Current Limitations
 - currently (2025-03-31) requires functionality in or called from program.cs/Program/ProcessFileAsync() to do the desired work.<br>
 
 <span style="color:red">
 <center>This is a NOT a build-and-run repo<br><br>IOW, you need to program what you want done to each file, and then build and run it</center></span><br>

   - this will be remedied <a href="https://tvtropes.org/pmwiki/pmwiki.php/Main/RealSoonNow" target="_blank">Real Soon Now</a> with the ability to exec a shell commmand, script, executable, etc., just like a Grown-up Program!
 - Testing?  What's that? (built and known to run, at least, on late model Windows 10)
 - The load measurement and throttling code hasn't been tested and might (likely) or might not (unlikely) hose your system with too many threads.  Remedy:  def get a better computer with all-SSD storage
 - This is almost certainly poky AF on spinning disks, but since I don't have any of those, I don't know
 - I don't know if this is a limitation, but currently it doesn't bypass things like the "standard" (i.e. unreadable) files like pagefile.sys, \Windows, etc.  (There *is*  extensive (LOL) exception handling regarding failed open-for-read)

## Features

- **High-performance file system traversal** with optimized CPU and I/O usage
- **Proper handling of symbolic links** to prevent recursive loops
- **File association gathering** to identify file extensions and their associated applications
- **Multi-threaded processing** that scales with available processors
- **I/O-aware throttling** to optimize performance based on disk capabilities
- **Support for filtering files** by pattern

## Usage

    FileMunger [options]

### Options

| Option | Description |
|--------|-------------|
| `--directories`, `-dir`, `-d` <dirs> | Comma-separated list of directories to process<br>Special value 'all' processes all local drive roots<br>Default: current drive root |
| `--recursive`, `-r` [yes/no] | Process subdirectories recursively<br>Default: yes |
| `--filespec`, `-f` <pattern> | File specification pattern (Windows wildcards)<br>Default: *.* |
| `--verbosity`, `-v` <level> | Output verbosity level: high, medium, low, none<br>Default: low |
| `--help`, `-h`, `-?` | Show help message |

### Examples

    FileMunger --directories C:\Data,D:\Backup --filespec *.docx
    FileMunger -d all -r no -v high
    FileMunger C:\Data

## Output

FileMunger produces both console output and a `FileAssociations.txt` file containing details about all file extensions found during processing, including:

- The file extension (.txt, .exe, etc.)
- The registered file type
- The friendly name of the file type
- The MIME content type
- The perceived file type
- The command used to open files with this extension

## Performance

FileMunger is optimized for high performance:

- Automatically scales processing threads based on CPU cores
- Throttles I/O operations based on disk performance
- Uses reader-writer locks for efficient concurrent access
- Processes different physical drives in parallel
- Uses efficient memory management for large directory trees

## Requirements

- Windows operating system
- .NET 9.0 or later
- Administrator rights (for accessing certain system directories)

## Building from Source

To build FileMunger from source:

    dotnet build -c Release

## Technical Notes

### Thread Safety

FileMunger employs several techniques for thread safety:

- `ReaderWriterLockSlim` for efficient concurrent read/exclusive write operations
- `Interlocked` operations for atomic counter updates
- `ConcurrentDictionary` for thread-safe collections
- TPL Dataflow for parallel processing with controlled concurrency

### Symbolic Link Handling

FileMunger safely traverses symbolic links by:

1. Detecting reparse points using FileAttributes
2. Resolving targets using Windows API calls
3. Tracking visited targets to avoid cycles
4. Supporting directory and file symbolic links

### I/O Performance Optimization

The I/O performance throttling dynamically adjusts concurrency based on:

1. Current disk read/write throughput
2. Available system resources
3. The number of logical processors

## License

[MIT License](LICENSE)

## Contributors

- Lynne Whitehorn (https://github.com/lynnewu)

---

  [1]: https://tvtropes.org/pmwiki/pmwiki.php/Main/RealSoonNow
