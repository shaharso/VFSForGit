using System;
using System.Collections.Generic;
using System.Threading;
using GVFS.Common.NamedPipes;
using GVFS.Common.Tracing;
using GVFS.Virtualization.Background;
using GVFS.Virtualization.BlobSize;

namespace GVFS.Virtualization.Projection
{
    public interface IGitIndexProjection : IDisposable, IProfilerOnlyIndexProjection
    {
        int EstimatedPlaceholderCount { get; }
        void BuildProjectionFromPath(ITracer tracer, string indexPath);
        void Initialize(BackgroundFileSystemTaskRunner backgroundFileSystemTaskRunner);
        void Shutdown();
        void WaitForProjectionUpdate();
        NamedPipeMessages.ReleaseLock.Response TryReleaseExternalLock(int pid);
        bool IsProjectionParseComplete();
        void InvalidateProjection();
        void InvalidateModifiedFiles();
        void OnPlaceholderCreateBlockedForGit();
        void ClearNegativePathCacheIfPollutedByGit();
        void OnPlaceholderFolderCreated(string virtualPath);
        void OnPlaceholderFolderExpanded(string relativePath);
        void OnPlaceholderFileCreated(string virtualPath, string sha);
        bool TryGetProjectedItemsFromMemory(string folderPath, out List<ProjectedFileInfo> projectedItems);
        void GetFileTypeAndMode(string filePath, out GitIndexProjection.FileType fileType, out ushort fileMode);

        List<ProjectedFileInfo> GetProjectedItems(
            CancellationToken cancellationToken,
            BlobSizes.BlobSizesConnection blobSizesConnection,
            string folderPath);

        bool IsPathProjected(string virtualPath, out string fileName, out bool isFolder);

        ProjectedFileInfo GetProjectedFileInfo(
            CancellationToken cancellationToken,
            BlobSizes.BlobSizesConnection blobSizesConnection,
            string virtualPath,
            out string parentFolderPath);

        FileSystemTaskResult OpenIndexForRead();
        FileSystemTaskResult CloseIndex();
        FileSystemTaskResult AddMissingModifiedFiles();
        void RemoveFromPlaceholderList(string fileOrFolderPath);
    }
}