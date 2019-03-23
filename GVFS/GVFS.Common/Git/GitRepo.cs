﻿using GVFS.Common.FileSystem;
using GVFS.Common.Tracing;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace GVFS.Common.Git
{
    public class GitRepo : IDisposable
    {
        private static readonly byte[] LooseBlobHeader = new byte[] { (byte)'b', (byte)'l', (byte)'o', (byte)'b', (byte)' ' };

        private ITracer tracer;
        private PhysicalFileSystem fileSystem;
        private LibGit2RepoInvoker libgit2RepoInvoker;
        private Enlistment enlistment;

        public GitRepo(ITracer tracer, Enlistment enlistment, PhysicalFileSystem fileSystem, Func<LibGit2Repo> repoFactory = null)
        {
            this.tracer = tracer;
            this.enlistment = enlistment;
            this.fileSystem = fileSystem;

            this.GVFSLock = new GVFSLock(tracer);

            this.libgit2RepoInvoker = new LibGit2RepoInvoker(
                tracer,
                repoFactory ?? (() => new LibGit2Repo(this.tracer, this.enlistment.WorkingDirectoryRoot)));
        }

        // For Unit Testing
        protected GitRepo(ITracer tracer)
        {
            this.GVFSLock = new GVFSLock(tracer);
        }

        private enum LooseBlobState
        {
            Invalid,
            Missing,
            Exists,
            Corrupt,
            Unknown,
        }

        public bool HasActiveLibGit2Repo => this.libgit2RepoInvoker?.IsActive == true;

        public GVFSLock GVFSLock
        {
            get;
            private set;
        }

        public bool TryGetIsBlob(string sha, out bool isBlob)
        {
            return this.libgit2RepoInvoker.TryInvoke(repo => repo.IsBlob(sha), out isBlob);
        }

        public virtual bool TryCopyBlobContentStream(string blobSha, Action<Stream, long> writeAction)
        {
            // if (blobSha != null && blobSha.Contains(".cs"))
            {
                string path = Path.Combine(@"c:\gvfs\normalGit", blobSha);
                if (File.Exists(path))
                {
                    var fileStream = File.OpenRead(path);
                    writeAction.Invoke(fileStream, fileStream.Length);
                    return true;
                }

                return false;
            }

            // LooseBlobState state = this.GetLooseBlobState(blobSha, writeAction, out long size);
            // if (state == LooseBlobState.Exists)
            // {
            //     return true;
            // }
            // else if (state != LooseBlobState.Missing)
            // {
            //     return false;
            // }
            //
            // if (!this.libgit2RepoInvoker.TryInvoke(repo => repo.TryCopyBlob(blobSha, writeAction), out bool copyBlobResult))
            // {
            //     return false;
            // }
            //
            // return copyBlobResult;
        }

        public virtual bool CommitAndRootTreeExists(string commitSha)
        {
            bool output = false;
            this.libgit2RepoInvoker.TryInvoke(repo => repo.CommitAndRootTreeExists(commitSha), out output);
            return output;
        }

        public virtual bool ObjectExists(string blobSha)
        {
            bool output = false;
            this.libgit2RepoInvoker.TryInvoke(repo => repo.ObjectExists(blobSha), out output);
            return output;
        }

        /// <summary>
        /// Try to find the size of a given blob by SHA1 hash.
        ///
        /// Returns true iff the blob exists as a loose object.
        /// </summary>
        public virtual bool TryGetBlobLength(string blobSha, out long size)
        {
            return this.GetLooseBlobState(blobSha, null, out size) == LooseBlobState.Exists;
        }

        public void Dispose()
        {
            if (this.libgit2RepoInvoker != null)
            {
                this.libgit2RepoInvoker.Dispose();
                this.libgit2RepoInvoker = null;
            }
        }

        private static bool ReadLooseObjectHeader(Stream input, out long size)
        {
            size = 0;

            byte[] buffer = new byte[5];
            input.Read(buffer, 0, buffer.Length);
            if (!Enumerable.SequenceEqual(buffer, LooseBlobHeader))
            {
                return false;
            }

            while (true)
            {
                int v = input.ReadByte();
                if (v == -1)
                {
                    return false;
                }

                if (v == '\0')
                {
                    break;
                }

                size = (size * 10) + (v - '0');
            }

            return true;
        }

        private LooseBlobState GetLooseBlobStateAtPath(string blobPath, Action<Stream, long> writeAction, out long size)
        {
            bool corruptLooseObject = false;
            try
            {
                if (this.fileSystem.FileExists(blobPath))
                {
                    using (Stream file = this.fileSystem.OpenFileStream(blobPath, FileMode.Open, FileAccess.Read, FileShare.Read, callFlushFileBuffers: false))
                    {
                        // The DeflateStream header starts 2 bytes into the gzip header, but they are otherwise compatible
                        file.Position = 2;
                        using (DeflateStream deflate = new DeflateStream(file, CompressionMode.Decompress))
                        {
                            if (!ReadLooseObjectHeader(deflate, out size))
                            {
                                corruptLooseObject = true;
                                return LooseBlobState.Corrupt;
                            }

                            writeAction?.Invoke(deflate, size);
                            return LooseBlobState.Exists;
                        }
                    }
                }

                size = -1;
                return LooseBlobState.Missing;
            }
            catch (InvalidDataException ex)
            {
                corruptLooseObject = true;

                EventMetadata metadata = new EventMetadata();
                metadata.Add("blobPath", blobPath);
                metadata.Add("Exception", ex.ToString());
                this.tracer.RelatedWarning(metadata, nameof(this.GetLooseBlobStateAtPath) + ": Failed to stream blob (InvalidDataException)", Keywords.Telemetry);

                size = -1;
                return LooseBlobState.Corrupt;
            }
            catch (IOException ex)
            {
                EventMetadata metadata = new EventMetadata();
                metadata.Add("blobPath", blobPath);
                metadata.Add("Exception", ex.ToString());
                this.tracer.RelatedWarning(metadata, nameof(this.GetLooseBlobStateAtPath) + ": Failed to stream blob from disk", Keywords.Telemetry);

                size = -1;
                return LooseBlobState.Unknown;
            }
            finally
            {
                if (corruptLooseObject)
                {
                    string corruptBlobsFolderPath = Path.Combine(this.enlistment.EnlistmentRoot, GVFSConstants.DotGVFS.CorruptObjectsPath);
                    string corruptBlobPath = Path.Combine(corruptBlobsFolderPath, Path.GetRandomFileName());

                    EventMetadata metadata = new EventMetadata();
                    metadata.Add("blobPath", blobPath);
                    metadata.Add("corruptBlobPath", corruptBlobPath);
                    metadata.Add(TracingConstants.MessageKey.InfoMessage, nameof(this.GetLooseBlobStateAtPath) + ": Renaming corrupt loose object");
                    this.tracer.RelatedEvent(EventLevel.Informational, nameof(this.GetLooseBlobStateAtPath) + "_RenameCorruptObject", metadata);

                    try
                    {
                        this.fileSystem.CreateDirectory(corruptBlobsFolderPath);
                        this.fileSystem.MoveFile(blobPath, corruptBlobPath);
                    }
                    catch (Exception e)
                    {
                        metadata = new EventMetadata();
                        metadata.Add("blobPath", blobPath);
                        metadata.Add("blobBackupPath", corruptBlobPath);
                        metadata.Add("Exception", e.ToString());
                        metadata.Add(TracingConstants.MessageKey.WarningMessage, nameof(this.GetLooseBlobStateAtPath) + ": Failed to rename corrupt loose object");
                        this.tracer.RelatedEvent(EventLevel.Warning, nameof(this.GetLooseBlobStateAtPath) + "_RenameCorruptObjectFailed", metadata, Keywords.Telemetry);
                    }
                }
            }
        }

        private LooseBlobState GetLooseBlobState(string blobSha, Action<Stream, long> writeAction, out long size)
        {
            string blobPath = Path.Combine(
                this.enlistment.GitObjectsRoot,
                blobSha.Substring(0, 2),
                blobSha.Substring(2));

            LooseBlobState state = this.GetLooseBlobStateAtPath(blobPath, writeAction, out size);
            if (state == LooseBlobState.Missing)
            {
                blobPath = Path.Combine(
                   this.enlistment.LocalObjectsRoot,
                   blobSha.Substring(0, 2),
                   blobSha.Substring(2));
                state = this.GetLooseBlobStateAtPath(blobPath, writeAction, out size);
            }

            return state;
        }
    }
}
