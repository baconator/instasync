package ml.ohca.experiments

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.perforce.p4java.core.file.FileSpecBuilder
import com.perforce.p4java.core.file.IFileSpec
import com.perforce.p4java.option.UsageOptions
import com.perforce.p4java.option.server.GetChangelistsOptions
import com.perforce.p4java.option.server.GetFileContentsOptions
import com.perforce.p4java.option.server.GetFileSizesOptions
import com.perforce.p4java.server.IOptionsServer
import com.perforce.p4java.server.ServerFactory
import jnr.ffi.Platform
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Pointer
import jnr.ffi.types.off_t
import jnr.ffi.types.size_t
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.FuseFillDir
import ru.serce.jnrfuse.FuseStubFS
import ru.serce.jnrfuse.struct.FileStat
import ru.serce.jnrfuse.struct.FuseFileInfo
import ru.serce.jnrfuse.struct.Statvfs
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.*
import java.util.stream.Collectors

fun fileSpecs(vararg paths: String): List<IFileSpec> = FileSpecBuilder.makeFileSpecList(*paths)

sealed class FileInfo {
    abstract val isDirectory: Boolean

    data class PerforceInfo(val filespec: IFileSpec) : FileInfo() {
        override val isDirectory: Boolean
            get() = false
    }

    data class FilesystemInfo(val path: Path) : FileInfo() {
        override val isDirectory: Boolean
            get() = Files.isDirectory(path)
    }
}

class VirtualFS(val perforceServer: IOptionsServer, val overrideRoot: Path, val searchPath: String) : FuseStubFS() {
    private val fileContents = mutableMapOf<String, ByteArray>()

    private val mostRecentChangelist by lazy {
        perforceServer.getChangelists(fileSpecs(searchPath), GetChangelistsOptions().apply { maxMostRecent = 1; })
            .first().id
    }

    private val perforceFilespecCache = CacheBuilder
        .newBuilder()
        .maximumSize(10000)
        .build(object : CacheLoader<String, Map<String, FileInfo>>() {
            override fun load(key: String): Map<String, FileInfo> {
                val perforcePaths = perforceServer
                    .getDepotFiles(fileSpecs(key), false)
                    .map { it.depotPathString.substring(1) to FileInfo.PerforceInfo(it) }
                return perforcePaths
                    .toMap()
            }
        })

    private val files: Map<String, FileInfo>
        get() {
            val perforcePaths = perforceFilespecCache["$searchPath@$mostRecentChangelist"]
            try {
                val localPaths = Files.walk(overrideRoot, Int.MAX_VALUE)
                    .map { inputPath ->
                        val output = mutableListOf<String>()
                        var current = inputPath
                        while (current != overrideRoot) {
                            output.add(current.fileName.toString())
                            current = current.parent
                        }
                        output.reverse()
                        "/" + output.joinToString("/") to FileInfo.FilesystemInfo(inputPath)
                    }.filter {
                        it.first != "/"
                    }.collect(Collectors.toSet())

                return perforcePaths + localPaths
            } catch (e: Exception) {
                throw e
            }
        }

    override fun truncate(path: String, size: Long): Int {
        val fileInfo = files[path]
        return when (fileInfo) {
            is FileInfo.PerforceInfo -> TODO()
            is FileInfo.FilesystemInfo -> {
                Files.write(fileInfo.path, ByteArray(size.toInt()), StandardOpenOption.TRUNCATE_EXISTING)
                0
            }
            null -> -ErrorCodes.ENOENT()
        }
    }

    private val perforceFileSizeCache = mutableMapOf<String, Long>()
    private fun getFileSize(path: String): Long {
        val fileInfo = files.getValue(path)
        return when (fileInfo) {
            is FileInfo.PerforceInfo -> perforceFileSizeCache.computeIfAbsent(path) {
                perforceServer.getFileSizes(
                    listOf(
                        fileInfo.filespec
                    ), GetFileSizesOptions().setMaxFiles(1)
                ).first().fileSize
            }
            is FileInfo.FilesystemInfo -> Files.size(fileInfo.path)
        }
    }

    override fun statfs(path: String?, stbuf: Statvfs?): Int {
        if (Platform.getNativePlatform().os == WINDOWS) { // Stolen wholesale from the Internet. TODO: implement me D:
            if ("/" == path) {
                stbuf!!.f_blocks.set(1024 * 1024) // total data blocks in file system
                stbuf.f_frsize.set(1024)        // fs block size
                stbuf.f_bfree.set(1024 * 1024)  // free blocks in fs
            }
        }
        return super.statfs(path, stbuf)
    }

    override fun unlink(path: String): Int {
        val fileInfo = files[path]
        return when (fileInfo) {
            is FileInfo.PerforceInfo -> TODO()
            is FileInfo.FilesystemInfo -> {
                Files.delete(fileInfo.path)
                0
            }
            null -> -ErrorCodes.ENOENT()
        }
    }

    override fun mkdir(path: String, mode: Long): Int {
        return try {
            val targetDirectory = overrideRoot.resolve(".$path")
            if (!Files.exists(targetDirectory)) {
                Files.createDirectories(targetDirectory)
                0
            } else {
                -ErrorCodes.EEXIST()
            }
        } catch (e: IOException) {
            -ErrorCodes.ENOENT()
        }
    }

    override fun write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int {
        val fileInfo = files[path]
        val filePath = when (fileInfo) {
            is FileInfo.FilesystemInfo -> fileInfo.path
            is FileInfo.PerforceInfo, null -> {
                val localPath = overrideRoot.resolve(".$path")
                Files.write(localPath, downloadFile(path))
                localPath
            }
        }
        val temporaryDataArray = ByteArray(size.toInt())
        buf.get(0, temporaryDataArray, 0, size.toInt())
        try {
            Files.newByteChannel(filePath, setOf(StandardOpenOption.WRITE)).use {
                it.position(offset)
                it.write(ByteBuffer.wrap(temporaryDataArray))
            }
        } catch (e: IOException) {
            println(e)
        }
        return size.toInt()
    }


    override fun getattr(path: String, stat: FileStat): Int {
        val fileInfo = files[path]
        return if (fileInfo == null) {
            val filesInDirectory = listDirectory(path)
            if (filesInDirectory != null) {
                handleDirectory(stat)
            } else {
                -ErrorCodes.ENOENT()
            }
        } else {
            when (fileInfo) {
                is FileInfo.PerforceInfo -> handleFile(stat, path)
                is FileInfo.FilesystemInfo -> {
                    if (Files.isDirectory(fileInfo.path)) {
                        handleDirectory(stat)
                    } else {
                        handleFile(stat, path)
                    }
                }
            }
        }
    }

    private fun handleFile(stat: FileStat, path: String): Int {
        stat.st_mode.set(FileStat.S_IFREG or FileStat.ALL_READ or FileStat.ALL_WRITE or FileStat.S_IXUGO)
        val fileSize = getFileSize(path)
        stat.st_size.set(fileSize)
        stat.st_uid.set(context.uid.get())
        stat.st_gid.set(context.gid.get())
        return 0
    }

    private fun handleDirectory(stat: FileStat): Int {
        stat.st_mode.set(FileStat.S_IFDIR or FileStat.ALL_READ or FileStat.ALL_WRITE or FileStat.S_IXUGO)
        stat.st_uid.set(context.uid.get())
        stat.st_gid.set(context.gid.get())
        return 0
    }

    private fun listDirectory(path: String): List<String>? {
        val normalizedPath = if (path == "/") { "" } else { path }
        val pathPattern = Regex("^$normalizedPath(?:(?:/([^/]+))|$)")
        val matches = files.keys
            .mapNotNull { pathPattern.find(it) }
        return if (matches.isEmpty()) {
            null
        } else {
            try {
                matches
                    .mapNotNull { it.groups[1]?.value }
                    .distinct()
            } catch (e: Exception) {
                throw e
            }
        }
    }

    override fun readdir(path: String, buf: Pointer, filter: FuseFillDir, @off_t offset: Long, fi: FuseFileInfo): Int {
        val fileInfo = files[path]
        val files = listDirectory(path)

        if (files == null) {
            when (fileInfo) {
                is FileInfo.PerforceInfo, null -> return -ErrorCodes.ENOENT()
                is FileInfo.FilesystemInfo -> {
                    if (Files.notExists(fileInfo.path)) {
                        return -ErrorCodes.ENOENT()
                    }
                }
            }
        }

        filter.apply(buf, ".", null, 0)
        filter.apply(buf, "..", null, 0)
        files?.forEach { file -> filter.apply(buf, file, null, 0) }

        return 0
    }

    override fun open(path: String, fi: FuseFileInfo): Int {
        return if (path !in files) {
            -ErrorCodes.ENOENT()
        } else 0
    }

    override fun create(path: String?, mode: Long, fi: FuseFileInfo?): Int {
        return try {
            val targetFilePath = overrideRoot.resolve(".$path")
            if (!Files.exists(targetFilePath)) {
                if (Files.notExists(targetFilePath.parent)) {
                    Files.createDirectories(targetFilePath.parent)
                }
                Files.createFile(targetFilePath)
                0
            } else {
                -ErrorCodes.EEXIST()
            }
        } catch (e: IOException) {
            -ErrorCodes.ENOENT()
        }
    }

    override fun read(path: String, buf: Pointer, @size_t inSize: Long, @off_t offset: Long, fi: FuseFileInfo): Int {
        var size = inSize
        if (path !in files) {
            return -ErrorCodes.ENOENT()
        }

        val bytes = downloadFile(path)
        val length = bytes.size
        if (offset < length) {
            if (offset + size > length) {
                size = length - offset
            }
            buf.put(0, bytes, 0, bytes.size)
        } else {
            size = 0
        }
        return size.toInt()
    }

    private fun downloadFile(path: String): ByteArray {
        val fileInfo = files.getValue(path)
        return when (fileInfo) {
            is FileInfo.PerforceInfo -> fileContents.computeIfAbsent(path) {
                perforceServer.getFileContents(
                    listOf(fileInfo.filespec),
                    GetFileContentsOptions().apply { isNoHeaderLine = true; isDontAnnotateFiles = false }).readBytes()
            }
            is FileInfo.FilesystemInfo -> Files.readAllBytes(fileInfo.path)
        }
    }

}


fun main(args: Array<String>) {
    val clientName = args[0] // Will _eventually_ be used for submitting CLs.
    val windowsMountPath = "L:\\"
    val perforceServerUri = "workshop.perforce.com:1666"
    val instasyncName = "instasync"
    val instasyncVersion = "alpha0.1"
    val overrideRoot = Paths.get("G:/Scratch/overrides")
    val targetDepotPath = "//guest/perforce_software/p4/2018-1/..."

    val fs = FileSystems.getDefault()

    val mountPath = fs.getPath(windowsMountPath)
    val homePath = fs.getPath(System.getProperty("user.home"))
    val ticketsPath = homePath.resolve("p4tickets.txt")

    if (!Files.exists(ticketsPath)) {
        println("Couldn't find your p4tickets file (in $ticketsPath). Make sure you've logged into Perforce on the command line!")
        return
    }

    val perforceServer = ServerFactory.getOptionsServer(
        "p4java://$perforceServerUri",
        null,
        UsageOptions(null).apply {
            programName = instasyncName
            programVersion = instasyncVersion
        }
    ).apply { connect() }

    val tickets = (Files
        .readAllLines(ticketsPath)
        ?.mapNotNull { "^.+=(.+?):(.+)$".toRegex().matchEntire(it)?.destructured }
        ?: throw IllegalStateException("Couldn't parse tickets file!"))
    tickets.find { (username, authTicket) ->
        perforceServer.run {
            setAuthTicket(authTicket)
            userName = username
        }
        perforceServer.connect()
        "ticket expires in" in perforceServer.loginStatus
    } ?: throw IllegalArgumentException("Couldn't authenticate to Perforce server!")

    val stub = VirtualFS(perforceServer, overrideRoot, targetDepotPath)
    try {
        stub.mount(mountPath, true, true)
    } finally {
        stub.umount()
    }
}