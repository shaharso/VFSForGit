using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using GVFS.Common;
using GVFS.Common.Git;
using GVFS.Common.NamedPipes;
using GVFS.Common.Tracing;
using GVFS.Virtualization.Background;
using GVFS.Virtualization.BlobSize;
using GVFS.Virtualization.FileSystem;

namespace GVFS.Virtualization.Projection
{
    public class MirrorFolderIndexProjection : IGitIndexProjection
    {
        private ReaderWriterLockSlim projectionReadWriteLock;
        private ManualResetEventSlim projectionParseComplete;
        private GVFSContext context;

        private ConcurrentHashSet<string> updatePlaceholderFailures;
        private ConcurrentHashSet<string> deletePlaceholderFailures;

        private FileSystemVirtualizer fileSystemVirtualizer;

        private BackgroundFileSystemTaskRunner backgroundFileSystemTaskRunner;
        private ConcurrentDictionary<string, FolderInfo> folderInfos = new ConcurrentDictionary<string, FolderInfo>();

        // Cache of folder paths (in Windows format) to folder data
        private ConcurrentDictionary<string, GitIndexProjection.FolderData> projectionFolderCache = new ConcurrentDictionary<string, GitIndexProjection.FolderData>(StringComparer.OrdinalIgnoreCase);

        // Number of times that the negative path cache has (potentially) been updated by GVFS preventing
        // git from creating a placeholder (since the last time the cache was cleared)
        private int negativePathCacheUpdatedForGitCount;

        public MirrorFolderIndexProjection(GVFSContext context, FileSystemVirtualizer fileSystemVirtualizer)
        {
            this.context = context;
            this.projectionReadWriteLock = new ReaderWriterLockSlim();
            this.projectionParseComplete = new ManualResetEventSlim(initialState: false);

            this.fileSystemVirtualizer = fileSystemVirtualizer;
        }

        public int EstimatedPlaceholderCount => 0;

        public void Dispose()
        {
            if (this.projectionReadWriteLock != null)
            {
                this.projectionReadWriteLock.Dispose();
                this.projectionReadWriteLock = null;
            }

            if (this.projectionParseComplete != null)
            {
                this.projectionParseComplete.Dispose();
                this.projectionParseComplete = null;
            }
        }

        public void ForceRebuildProjection()
        {
        }

        public void ForceAddMissingModifiedPaths(ITracer tracer)
        {
        }

        public void BuildProjectionFromPath(ITracer tracer, string indexPath)
        {
        }

        public void Initialize(BackgroundFileSystemTaskRunner backgroundFileSystemTaskRunner)
        {
            this.backgroundFileSystemTaskRunner = backgroundFileSystemTaskRunner;

            this.ClearUpdatePlaceholderErrors();

            GitIndexProjection.SortedFolderEntries.InitializePools(this.context.Tracer, 1000);
            GitIndexProjection.LazyUTF8String.InitializePools(this.context.Tracer, 1000);

            this.folderInfos = new ConcurrentDictionary<string, FolderInfo>(this.GetFolderInfos(@"C:\gvfs\normalGit", @"C:\gvfs\normalGit").ToDictionary(fi=>fi.Path.ToString(), fi=>fi));
        }

        public void Shutdown()
        {
        }

        public void WaitForProjectionUpdate()
        {
            this.projectionParseComplete.Wait();
        }

        public NamedPipeMessages.ReleaseLock.Response TryReleaseExternalLock(int pid)
        {
            NamedPipeMessages.LockData externalHolder = this.context.Repository.GVFSLock.GetExternalHolder();
            if (externalHolder != null &&
                externalHolder.PID == pid)
            {
                // We MUST NOT release the lock until all processing has been completed, so that once
                // control returns to the user, the projection is in a consistent state

                this.context.Tracer.RelatedEvent(EventLevel.Informational, "ReleaseExternalLockRequested", null);
                this.context.Repository.GVFSLock.Stats.RecordReleaseExternalLockRequested();

                this.ClearNegativePathCacheIfPollutedByGit();

                ConcurrentHashSet<string> updateFailures = this.updatePlaceholderFailures;
                ConcurrentHashSet<string> deleteFailures = this.deletePlaceholderFailures;
                this.ClearUpdatePlaceholderErrors();

                if (this.context.Repository.GVFSLock.ReleaseLockHeldByExternalProcess(pid))
                {
                    if (updateFailures.Count > 0 || deleteFailures.Count > 0)
                    {
                        return new NamedPipeMessages.ReleaseLock.Response(
                            NamedPipeMessages.ReleaseLock.SuccessResult,
                            new NamedPipeMessages.ReleaseLock.ReleaseLockData(
                                new List<string>(updateFailures),
                                new List<string>(deleteFailures)));
                    }

                    return new NamedPipeMessages.ReleaseLock.Response(NamedPipeMessages.ReleaseLock.SuccessResult);
                }
            }

            this.context.Tracer.RelatedError("GitIndexProjection: Received a release request from a process that does not own the lock (PID={0})", pid);
            return new NamedPipeMessages.ReleaseLock.Response(NamedPipeMessages.ReleaseLock.FailureResult);
        }

        public bool IsProjectionParseComplete()
        {
            return this.projectionParseComplete.IsSet;
        }

        public virtual void InvalidateProjection()
        {
            // this.context.Tracer.RelatedEvent(EventLevel.Informational, "InvalidateProjection", null);
            //
            // this.projectionParseComplete.Reset();
            //
            // try
            // {
            //     // Because the projection is now invalid, attempt to delete the projection file.  If this delete fails
            //     // replacing the projection will be handled by the parsing thread
            //     this.context.FileSystem.DeleteFile(this.projectionIndexBackupPath);
            // }
            // catch (Exception e)
            // {
            //     EventMetadata metadata = CreateEventMetadata(e);
            //     metadata.Add(TracingConstants.MessageKey.InfoMessage, nameof(this.InvalidateProjection) + ": Failed to delete GVFS_Projection file");
            //     this.context.Tracer.RelatedEvent(EventLevel.Informational, nameof(this.InvalidateProjection) + "_FailedToDeleteProjection", metadata);
            // }
            //
            // this.SetProjectionAndPlaceholdersAsInvalid();
            // this.wakeUpIndexParsingThread.Set();
        }

        public void InvalidateModifiedFiles()
        {
        }

        public void OnPlaceholderCreateBlockedForGit()
        {
            int count = Interlocked.Increment(ref this.negativePathCacheUpdatedForGitCount);
            if (count == 1)
            {
                // If placeholder creation is blocked multiple times, only queue a single background task
                this.backgroundFileSystemTaskRunner.Enqueue(FileSystemTask.OnPlaceholderCreationsBlockedForGit());
            }
        }

        public void ClearNegativePathCacheIfPollutedByGit()
        {
            int count = Interlocked.Exchange(ref this.negativePathCacheUpdatedForGitCount, 0);
            if (count > 0)
            {
                this.ClearNegativePathCache();
            }
        }

        public void OnPlaceholderFolderCreated(string virtualPath)
        {
            // this.placeholderList.AddAndFlushFolder(virtualPath, isExpanded: false);
        }

        public virtual void OnPlaceholderFolderExpanded(string relativePath)
        {
            // this.placeholderList.AddAndFlushFolder(relativePath, isExpanded: true);
        }

        public virtual void OnPlaceholderFileCreated(string virtualPath, string sha)
        {
            // this.placeholderList.AddAndFlushFile(virtualPath, sha);
        }

        public bool TryGetProjectedItemsFromMemory(string folderPath, out List<ProjectedFileInfo> projectedItems)
        {
            projectedItems = null;

            this.projectionReadWriteLock.EnterReadLock();

            try
            {
                GitIndexProjection.FolderData folderData;
                if (this.TryGetOrAddFolderDataFromCache(folderPath, out folderData))
                {
                    if (folderData.ChildrenHaveSizes)
                    {
                        projectedItems = this.ConvertToProjectedFileInfos(folderData.ChildEntries);
                        return true;
                    }
                }

                return false;
            }
            finally
            {
                this.projectionReadWriteLock.ExitReadLock();
            }
        }

        public void GetFileTypeAndMode(string filePath, out GitIndexProjection.FileType fileType, out ushort fileMode)
        {
            throw new System.NotImplementedException();

            // if (!GVFSPlatform.Instance.FileSystem.SupportsFileMode)
            // {
            //     throw new InvalidOperationException($"{nameof(this.GetFileTypeAndMode)} is only supported on GVFSPlatforms that support file mode");
            // }
            //
            // fileType = GitIndexProjection.FileType.Regular;
            // fileMode = FileMode644;
            //
            // this.projectionReadWriteLock.EnterReadLock();
            //
            // try
            // {
            //     GitIndexProjection.FileTypeAndMode fileTypeAndMode;
            //     if (this.nonDefaultFileTypesAndModes.TryGetValue(filePath, out fileTypeAndMode))
            //     {
            //         fileType = fileTypeAndMode.Type;
            //         fileMode = fileTypeAndMode.Mode;
            //     }
            // }
            // finally
            // {
            //     this.projectionReadWriteLock.ExitReadLock();
            // }
        }

        public List<ProjectedFileInfo> GetProjectedItems(CancellationToken cancellationToken, BlobSizes.BlobSizesConnection blobSizesConnection, string folderPath)
        {
            this.projectionReadWriteLock.EnterReadLock();

            try
            {
                GitIndexProjection.FolderData folderData;
                if (this.TryGetOrAddFolderDataFromCache(folderPath, out folderData))
                {
                    // folderData.PopulateSizes(
                    //     this.context.Tracer,
                    //     this.gitObjects,
                    //     blobSizesConnection,
                    //     availableSizes: null,
                    //     cancellationToken: cancellationToken);

                    return this.ConvertToProjectedFileInfos(folderData.ChildEntries);
                }

                return new List<ProjectedFileInfo>();
            }
            finally
            {
                this.projectionReadWriteLock.ExitReadLock();
            }
        }

        public bool IsPathProjected(string virtualPath, out string fileName, out bool isFolder)
        {
            isFolder = false;
            string parentKey;
            this.GetChildNameAndParentKey(virtualPath, out fileName, out parentKey);
            GitIndexProjection.FolderEntryData data = this.GetProjectedFolderEntryData(
                blobSizesConnection: null,
                childName: fileName,
                parentKey: parentKey);

            if (data != null)
            {
                isFolder = data.IsFolder;
                return true;
            }

            return false;
        }

        public ProjectedFileInfo GetProjectedFileInfo(CancellationToken cancellationToken, BlobSizes.BlobSizesConnection blobSizesConnection, string virtualPath, out string parentFolderPath)
        {
            string childName;
            string parentKey;
            this.GetChildNameAndParentKey(virtualPath, out childName, out parentKey);
            parentFolderPath = parentKey;
            string gitCasedChildName;
            GitIndexProjection.FolderEntryData data = this.GetProjectedFolderEntryData(
                cancellationToken,
                blobSizesConnection,
                availableSizes: null,
                childName: childName,
                parentKey: parentKey,
                gitCasedChildName: out gitCasedChildName);

            if (data != null)
            {
                if (data.IsFolder)
                {
                    return new ProjectedFileInfo(gitCasedChildName, size: 0, isFolder: true, sha: Sha1Id.None);
                }
                else
                {
                    GitIndexProjection.FileData fileData = (GitIndexProjection.FileData)data;
                    return new ProjectedFileInfo(gitCasedChildName, fileData.Size, isFolder: false, sha: fileData.Sha);
                }
            }

            return null;
        }

        public FileSystemTaskResult OpenIndexForRead()
        {
            return FileSystemTaskResult.Success;
        }

        public FileSystemTaskResult CloseIndex()
        {
            return FileSystemTaskResult.Success;
        }

        public FileSystemTaskResult AddMissingModifiedFiles()
        {
            // try
            // {
            //     if (this.modifiedFilesInvalid)
            //     {
            //         using (ITracer activity = this.context.Tracer.StartActivity(
            //             nameof(this.indexParser.AddMissingModifiedFilesAndRemoveThemFromPlaceholderList),
            //             EventLevel.Informational))
            //         {
            //             FileSystemTaskResult result = this.indexParser.AddMissingModifiedFilesAndRemoveThemFromPlaceholderList(
            //                 activity,
            //                 this.indexFileStream);
            //
            //             if (result == FileSystemTaskResult.Success)
            //             {
            //                 this.modifiedFilesInvalid = false;
            //             }
            //
            //             return result;
            //         }
            //     }
            // }
            // catch (IOException e)
            // {
            //     EventMetadata metadata = CreateEventMetadata(e);
            //     this.context.Tracer.RelatedWarning(metadata, "IOException in " + nameof(this.AddMissingModifiedFiles) + " (Retryable)");
            //
            //     return FileSystemTaskResult.RetryableError;
            // }
            // catch (Exception e)
            // {
            //     EventMetadata metadata = CreateEventMetadata(e);
            //     this.context.Tracer.RelatedError(metadata, "Exception in " + nameof(this.AddMissingModifiedFiles) + " (FatalError)");
            //
            //     return FileSystemTaskResult.FatalError;
            // }

            return FileSystemTaskResult.Success;
        }

        public void RemoveFromPlaceholderList(string fileOrFolderPath)
        {
            // this.placeholderList.RemoveAndFlush(fileOrFolderPath);
        }

        protected void GetChildNameAndParentKey(string virtualPath, out string childName, out string parentKey)
        {
            parentKey = string.Empty;

            int separatorIndex = virtualPath.LastIndexOf(Path.DirectorySeparatorChar);
            if (separatorIndex < 0)
            {
                childName = virtualPath;
                return;
            }

            childName = virtualPath.Substring(separatorIndex + 1);
            parentKey = virtualPath.Substring(0, separatorIndex);
        }

        private void ClearUpdatePlaceholderErrors()
        {
            this.updatePlaceholderFailures = new ConcurrentHashSet<string>();
            this.deletePlaceholderFailures = new ConcurrentHashSet<string>();
        }

        private void ClearNegativePathCache()
        {
            uint totalEntryCount;
            FileSystemResult clearCacheResult = this.fileSystemVirtualizer.ClearNegativePathCache(out totalEntryCount);

            // int gitCount = Interlocked.Exchange(ref this.negativePathCacheUpdatedForGitCount, 0);
            // EventMetadata clearCacheMetadata = CreateEventMetadata();
            // clearCacheMetadata.Add(TracingConstants.MessageKey.InfoMessage, $"{nameof(this.ClearNegativePathCache)}: Cleared negative path cache");
            // clearCacheMetadata.Add(nameof(totalEntryCount), totalEntryCount);
            // clearCacheMetadata.Add("negativePathCacheUpdatedForGitCount", gitCount);
            // clearCacheMetadata.Add("clearCacheResult.Result", clearCacheResult.ToString());
            // clearCacheMetadata.Add("clearCacheResult.RawResult", clearCacheResult.RawResult);
            // this.context.Tracer.RelatedEvent(EventLevel.Informational, $"{nameof(this.ClearNegativePathCache)}_ClearedCache", clearCacheMetadata);

            if (clearCacheResult.Result != FSResult.Ok)
            {
                this.LogErrorAndExit("ClearNegativePathCache failed, exiting process. ClearNegativePathCache result: " + clearCacheResult.ToString());
            }
        }

        private void LogErrorAndExit(string message, Exception e = null)
        {
            // EventMetadata metadata = CreateEventMetadata(e);
            // this.context.Tracer.RelatedError(metadata, message);
            Environment.Exit(1);
        }

        /// <summary>
        /// Try to get the FolderData for the specified folder path from the projectionFolderCache
        /// cache.  If the  folder is not already in projectionFolderCache, search for it in the tree and
        /// then add it to projectionData
        /// </summary>
        /// <returns>True if the folder could be found, and false otherwise</returns>
        private bool TryGetOrAddFolderDataFromCache(
            string folderPath,
            out GitIndexProjection.FolderData folderData)
        {
            if (!this.projectionFolderCache.TryGetValue(folderPath, out folderData))
            {
                GitIndexProjection.LazyUTF8String[] pathParts = folderPath
                    .Split(new char[] { Path.DirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => new GitIndexProjection.LazyUTF8String(x))
                    .ToArray();

                GitIndexProjection.FolderEntryData data;
                if (!this.TryGetFolderEntryDataFromTree(folderPath, pathParts, folderEntryData: out data))
                {
                    folderPath = null;
                    return false;
                }

                if (data.IsFolder)
                {
                    folderData = (GitIndexProjection.FolderData)data;
                    this.projectionFolderCache.TryAdd(folderPath, folderData);
                }
                else
                {
                    // EventMetadata metadata = CreateEventMetadata();
                    // metadata.Add("folderPath", folderPath);
                    // metadata.Add(TracingConstants.MessageKey.InfoMessage, "Found file at path");
                    // this.context.Tracer.RelatedEvent(
                    //     EventLevel.Informational,
                    //     $"{nameof(this.TryGetOrAddFolderDataFromCache)}_FileAtPath",
                    //     metadata);

                    folderPath = null;
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Finds the FolderEntryData for the path provided
        /// </summary>
        /// <param name="pathParts">Path to desired entry</param>
        /// <param name="folderEntryData">Out: FolderEntryData for pathParts</param>
        /// <returns>True if the path could be found in the tree, and false otherwise</returns>
        /// <remarks>If the root folder is desired, the pathParts should be an empty array</remarks>
        private bool TryGetFolderEntryDataFromTree(string folderPath, GitIndexProjection.LazyUTF8String[] pathParts, out GitIndexProjection.FolderEntryData folderEntryData)
        {
            // var folderData = new GitIndexProjection.FolderData();
            // folderData.ResetData(new GitIndexProjection.LazyUTF8String("TestFolder"));
            // var sha1ForUtf8String = SHA1Util.SHA1ForUTF8String("TestFolder");
            // folderData.AddChildFile(new GitIndexProjection.LazyUTF8String("testFile.txt"), sha1ForUtf8String);
            //
            // folderEntryData = folderData;

            folderEntryData = null;

            FolderInfo folderInfo;
            if (!this.folderInfos.TryGetValue(folderPath, out folderInfo))
            {
                return false;
            }

            folderEntryData = folderInfo.FolderData;
            return true;
        }

        private void ClearProjectionCaches()
        {
            GitIndexProjection.SortedFolderEntries.FreePool();
            GitIndexProjection.LazyUTF8String.FreePool();
            this.projectionFolderCache.Clear();

            // this.nonDefaultFileTypesAndModes.Clear();
            // this.rootFolderData.ResetData(new GitIndexProjection.LazyUTF8String("<root>"));
        }

        private List<ProjectedFileInfo> ConvertToProjectedFileInfos(GitIndexProjection.SortedFolderEntries sortedFolderEntries)
        {
            List<ProjectedFileInfo> childItems = new List<ProjectedFileInfo>(sortedFolderEntries.Count);
            for (int i = 0; i < sortedFolderEntries.Count; i++)
            {
                GitIndexProjection.FolderEntryData childEntry = sortedFolderEntries[i];

                if (childEntry.IsFolder)
                {
                    childItems.Add(new ProjectedFileInfo(childEntry.Name.GetString(), size: 0, isFolder: true, sha: Sha1Id.None));
                }
                else
                {
                    GitIndexProjection.FileData fileData = (GitIndexProjection.FileData)childEntry;
                    childItems.Add(new ProjectedFileInfo(fileData.Name.GetString(), fileData.Size, isFolder: false, sha: fileData.Sha));
                }
            }

            return childItems;
        }

        /// <summary>
        /// Get the FolderEntryData for the specified child name and parent key.
        /// </summary>
        /// <param name="blobSizesConnection">
        /// BlobSizesConnection used to lookup the size for the FolderEntryData.  If null, size will not be populated.
        /// </param>
        /// <param name="childName">Child name (i.e. file name)</param>
        /// <param name="parentKey">Parent key (parent folder path)</param>
        /// <returns>FolderEntryData for the specified childName and parentKey or null if no FolderEntryData exists for them in the projection</returns>
        /// <remarks><see cref="GetChildNameAndParentKey"/> can be used for getting child name and parent key from a file path</remarks>
        private GitIndexProjection.FolderEntryData GetProjectedFolderEntryData(
            BlobSizes.BlobSizesConnection blobSizesConnection,
            string childName,
            string parentKey)
        {
            string casedChildName;
            return this.GetProjectedFolderEntryData(
                CancellationToken.None,
                blobSizesConnection,
                availableSizes: null,
                childName: childName,
                parentKey: parentKey,
                gitCasedChildName: out casedChildName);
        }

        private GitIndexProjection.FolderEntryData GetProjectedFolderEntryData(
            CancellationToken cancellationToken,
            BlobSizes.BlobSizesConnection blobSizesConnection,
            Dictionary<string, long> availableSizes,
            string childName,
            string parentKey,
            out string gitCasedChildName)
        {
            this.projectionReadWriteLock.EnterReadLock();
            try
            {
                GitIndexProjection.FolderData parentFolderData;
                if (this.TryGetOrAddFolderDataFromCache(parentKey, out parentFolderData))
                {
                    GitIndexProjection.LazyUTF8String child = new GitIndexProjection.LazyUTF8String(childName);
                    GitIndexProjection.FolderEntryData childData;
                    if (parentFolderData.ChildEntries.TryGetValue(child, out childData))
                    {
                        gitCasedChildName = childData.Name.GetString();

                        if (blobSizesConnection != null && !childData.IsFolder)
                        {
                            GitIndexProjection.FileData fileData = (GitIndexProjection.FileData)childData;

                            // if (!fileData.IsSizeSet() && !fileData.TryPopulateSizeLocally(this.context.Tracer, this.gitObjects, blobSizesConnection, availableSizes, out string _))
                            // {
                            //     Stopwatch queryTime = Stopwatch.StartNew();
                            //     parentFolderData.PopulateSizes(this.context.Tracer, this.gitObjects, blobSizesConnection, availableSizes, cancellationToken);
                            //     this.context.Repository.GVFSLock.Stats.RecordSizeQuery(queryTime.ElapsedMilliseconds);
                            // }

                            if (!fileData.IsSizeSet())
                            {
                                var fullPath = Path.Combine(@"c:\gvfs\normalGit", parentKey, gitCasedChildName);
                                fileData.Size = new FileInfo(fullPath).Length;
                            }
                        }

                        return childData;
                    }
                }

                gitCasedChildName = string.Empty;
                return null;
            }
            finally
            {
                this.projectionReadWriteLock.ExitReadLock();
            }
        }

        private List<FolderInfo> GetFolderInfos(string absuluteFolder, string rootFolder)
        {
            var folderInfo = new GitIndexProjection.FolderData();
            var relativeFolderPath = this.GetRelativeFolderPath(rootFolder, absuluteFolder);
            var folderName = Path.GetFileNameWithoutExtension(relativeFolderPath);
            folderInfo.ResetData(new GitIndexProjection.LazyUTF8String(folderName));

            var dirs = Directory.GetDirectories(absuluteFolder);
            var files = Directory.GetFiles(absuluteFolder);

            foreach (var file in files)
            {
                var fileName = Path.GetFileName(file);
                var sha1 = SHA1Util.SHA1ForUTF8String(fileName);
                folderInfo.AddChildFile(new GitIndexProjection.LazyUTF8String(fileName), sha1);
            }

            foreach (var dir in dirs)
            {
                var dirName = Path.GetFileName(dir);
                folderInfo.AddChildFolder(new GitIndexProjection.LazyUTF8String(dirName));
            }

            var subfolders = dirs.SelectMany(d=>this.GetFolderInfos(d, rootFolder)).ToList();

            return subfolders.Concat(new[] { new FolderInfo { Path = relativeFolderPath, FolderData = folderInfo } }).ToList();
        }

        private string GetRelativeFolderPath(string root, string full)
        {
            var folderPathWithoutRootFolder = full.Replace(root, string.Empty);

            if (folderPathWithoutRootFolder == string.Empty)
            {
                return string.Empty;
            }

            var relativePath = folderPathWithoutRootFolder.TrimStart("\\".ToCharArray());

            return relativePath;
        }

        private class FolderInfo
        {
            public string Path { get; set; }

            public GitIndexProjection.FolderData FolderData { get; set; }
        }
    }
}