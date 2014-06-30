/*
 * Copyright 2008 OPENXTRA Limited
 * 
 * This file is part of TimeTag.
 * 
 * TimeTag is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * TimeTag is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with TimeTag.  If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Openxtra.TimeTag.Database;

namespace TimeTag.Core.Binary
{
    internal class BinaryFileTimeSeriesDatabaseDao : BinaryFileDao, ITimeSeriesDatabaseDao, IBinaryFileReaderWriter
    {
        /// <summary>
        /// Default value used when the version number isn't yet known.
        /// </summary>
        private const short UninitialisedVersionNumber = -1;

        /// <summary>
        /// The current file version number. If the version number isn't known 
        /// then it can be assumed that the database hasn't yet been read. Whenever 
        /// a change to the database image on disk happens, then the file version 
        /// must be incremented.
        /// </summary>
        private short currentFileVersion = 1;

        /// <summary>
        /// The signature written to the binary file to identify the file as a 
        /// TimeTag database file
        /// </summary>
        private const string ExpectedSignature = "PTD";

        /// <summary>
        /// Signature read from the database file to identify it as a TimeTag file
        /// </summary>
        private string signature;

        /// <summary>
        /// Version number read from the PTD binary file
        /// </summary>
        private Int16 versionNumber = UninitialisedVersionNumber;

        /// <summary>
        /// Full path to the PTD binary file
        /// </summary>
        private string filePath;

        // The database file stream
        private FileStream databaseFileStream;

        // The lock file stream
        private FileStream lockFileStream;

        private BinaryReader br;
        private BinaryWriter bw;

        /// <summary>
        /// Data source DAOs used to interact with the data sources on disk
        /// </summary>
        private BinaryFileDataSourceCollectionDao dataSources = new BinaryFileDataSourceCollectionDao();

        /// <summary>
        /// Archive template DAOs
        /// </summary>
        private BinaryFileArchiveTemplateCollectionDao archiveTemplates = new BinaryFileArchiveTemplateCollectionDao();

        /// <summary>
        /// DTO currently being worked on
        /// </summary>
        private TimeSeriesDatabaseDto dto;

        /// <summary>
        /// Fix-up table to data sources
        /// </summary>
        private BinaryFileFixupTable dataSourceFixupTable;

        /// <summary>
        /// Fix-up table to archive templates
        /// </summary>
        private BinaryFileFixupTable archiveTemplateFixupTable;

        // Start time DAO
        private BinaryFileDateTimeDao startTimeDao;

        // Title DAO
        private BinaryFileStringDao titleDao;

        public BinaryFileTimeSeriesDatabaseDao(string filePath)
        {
            this.filePath = filePath;
            this.dataSourceFixupTable = new BinaryFileFixupTable(this, DataSource.MaxNameLength);
            this.archiveTemplateFixupTable = new BinaryFileFixupTable(this, ArchiveTemplate.MaxNameLength);
        }

        public BinaryWriter Writer
        {
            get { return this.bw; }
        }

        public BinaryReader Reader
        {
            get { return this.br; }
        }

        public short VersionNumber
        {
            get { return this.versionNumber; }
            private set { this.versionNumber = value; }
        }

        public BinaryFileDataSourceCollectionDao DataSources
        {
            get { return this.dataSources; }
        }

        public void Connect(TimeSeriesDatabase.ConnectionMode mode)
        {
            if (IsConnected() != true)
            {
                switch (mode)
                {
                    case TimeSeriesDatabase.ConnectionMode.ReadOnly:
                        OpenRead();
                        break;

                    case TimeSeriesDatabase.ConnectionMode.ReadWrite:
                        OpenReadWrite();
                        break;

                    default:
                        Debug.Fail(string.Format("Unknown ConnectionMode {0}", mode));
                        break;
                }
            }
        }

        public void Close()
        {
            if (this.br != null)
            {
                this.br.Close();
                this.br = null;
            }

            if (this.bw != null)
            {
                this.bw.Close();
                this.bw = null;
            }

            if (this.databaseFileStream != null)
            {
                this.databaseFileStream.Close();
                this.databaseFileStream = null;
            }
            Unlock();
        }

        public bool IsConnected()
        {
            return this.databaseFileStream != null;
        }

        public bool IsValid()
        {
            // File signature
            if (this.signature != ExpectedSignature)
            {
                // The file isn't a TimeTag file
                return false;
            }

            // PTD file version
            if (this.versionNumber <= 0)
            {
                // The version number isn't valid
                return false;
            }

            // The file looks ok
            return true;
        }

        public void Create(TimeSeriesDatabaseDto dto)
        {
            this.dto = dto;
            OpenCreate();
            WriteHeader();
            WriteAttributes();
            this.archiveTemplateFixupTable.Create(CreateArchiveTemplateFixupDto(dto.ArchiveTemplates));
            this.dataSourceFixupTable.Create(CreateDataSourceFixupDto(dto.DataSources));
            this.dto = null;
        }

        public TimeSeriesDatabaseDto Read()
        {
            TimeSeriesDatabaseDto dto = this.dto = new TimeSeriesDatabaseDto();
            this.dto.Dao = this;
            ReadHeader();
            ReadAttributes();
            this.archiveTemplateFixupTable.Read();
            this.dataSourceFixupTable.Read();
            ReadArchiveTemplates();
            ReadDataSources();
            this.dto = null;
            return dto;
        }

        public void Lock()
        {
            Debug.Assert(this.lockFileStream == null);

            DeleteOrphanedLockFile();

            while (true)
            {
                // Wait for any other process/thread to unlock the database
                WaitForUnlock();

                try
                {
                    /*
                     * Create a lock file that cannot be overwritten by any other 
                     * thread/process
                     */
                    this.lockFileStream = new FileStream(
                        GetLockFilename(), FileMode.CreateNew, FileAccess.Write, FileShare.None, 512, FileOptions.DeleteOnClose
                        );

                    /* 
                     * Once the lock file has been successfully created so 
                     * that it cannot be overwritten by any other thread or process, 
                     * terminate the loop
                     */
                    break;
                }
                catch (Exception)
                {
                    /*
                     * A race condition occured when another process/thread 
                     * acquired the lock first before we could acquire the 
                     * lock, start to wait for the lock to be released again
                     */
                    continue;
                }
            }
        }

        public void Unlock()
        {
            Debug.Assert(IsLocked());
            Debug.Assert(this.lockFileStream != null);

            // The lock file is deleted on close
            this.lockFileStream.Close();
            this.lockFileStream = null;
        }

        public void SetTitle(string newTitle)
        {
            Debug.Assert(this.titleDao != null);

            this.titleDao.Update(newTitle);
        }

        public BinaryFileFixup FindDataSourceFixupByName(string dataSourceName)
        {
            return this.dataSourceFixupTable.FindByName(dataSourceName);
        }

        public BinaryFileFixup FindArchiveTemplateFixupByName(string archiveName)
        {
            return this.archiveTemplateFixupTable.FindByName(archiveName);
        }

        private BinaryFileFixupDto[] CreateDataSourceFixupDto(DataSourceDto[] dataSources)
        {
            List<BinaryFileFixupDto> fixupDtos = new List<BinaryFileFixupDto>();
            foreach (DataSourceDto dto in dataSources)
            {
                BinaryFileFixupDto fixupDto = new BinaryFileFixupDto();
                fixupDto.Name = dto.Name;
                fixupDtos.Add(fixupDto);
            }
            return fixupDtos.ToArray();
        }

        private BinaryFileFixupDto[] CreateArchiveTemplateFixupDto(ArchiveTemplateDto[] newArchiveTemplates)
        {
            List<BinaryFileFixupDto> fixupDtos = new List<BinaryFileFixupDto>();
            foreach (ArchiveTemplateDto dto in newArchiveTemplates)
            {
                BinaryFileFixupDto fixupDto = new BinaryFileFixupDto();
                fixupDto.Name = dto.Name;
                fixupDtos.Add(fixupDto);
            }
            return fixupDtos.ToArray();
        }

        private void ReadAttributes()
        {
            // Title
            if (this.titleDao == null)
            {
                this.titleDao = new BinaryFileStringDao(this, TimeSeriesDatabase.MaxTitleLength);
            }
            this.dto.Title = this.titleDao.Read();

            // Start time
            if (this.startTimeDao == null)
            {
                this.startTimeDao = new BinaryFileDateTimeDao(this);
            }
            this.dto.StartTime = this.startTimeDao.Read();
        }

        private void WriteHeader()
        {
            // Identifies the file as a PTD database
            this.bw.Write(ExpectedSignature);

            // PTD file version
            this.bw.Write(this.currentFileVersion);
        }

        private void WriteAttributes()
        {
            // Title
            if (this.titleDao == null)
            {
                this.titleDao = new BinaryFileStringDao(this, TimeSeriesDatabase.MaxTitleLength);
            }
            this.titleDao.Create(this.dto.Title);

            // Start time
            if (this.startTimeDao == null)
            {
                this.startTimeDao = new BinaryFileDateTimeDao(this);
            }
            this.startTimeDao.Create(this.dto.StartTime);
        }

        private void ReadHeader()
        {
            // Read the file signature
            this.signature = this.br.ReadString();

            // File version number
            VersionNumber = this.br.ReadInt16();
        }

        private void ReadDataSources()
        {
            foreach (BinaryFileFixup fixup in this.dataSourceFixupTable)
            {
                if (fixup.Position != BinaryFileDao.UnknownOffset)
                {
                    BinaryFileDataSourceDao dataSourceDao = new BinaryFileDataSourceDao(this);
                    dataSourceDao.Fixup = fixup;
                    DataSourceDto dataSourceDto = dataSourceDao.Read();
                    this.DataSources.Add(dataSourceDao);
                    this.dto.AddDataSource(dataSourceDto);
                }
            }
        }

        private void ReadArchiveTemplates()
        {
            foreach (BinaryFileFixup fixup in this.archiveTemplateFixupTable)
            {
                BinaryFileArchiveTemplateDao archiveTemplateDao = new BinaryFileArchiveTemplateDao(this);
                archiveTemplateDao.Fixup = fixup;
                ArchiveTemplateDto archiveTemplateDto = archiveTemplateDao.Read();
                this.archiveTemplates.Add(archiveTemplateDao);
                this.dto.AddArchiveTemplate(archiveTemplateDto);
            }
        }

        private void OpenRead()
        {
            Debug.Assert(this.databaseFileStream == null);
            Debug.Assert(this.br == null);
            Debug.Assert(this.bw == null);

            /* 
             * The lock on the database must be acquired prior to the  
             * database being opened to avoid dead lock
             */
            Lock();

            /*
             * Open the file for reading. Other processes may already have the 
             * file open for reading though no process may write to the file
             */
            this.databaseFileStream = File.OpenRead(this.filePath);

            Debug.Assert(this.databaseFileStream.CanRead);
            Debug.Assert(this.databaseFileStream.CanSeek);
            Debug.Assert(this.databaseFileStream.CanWrite != true);

            this.br = new BinaryReader(this.databaseFileStream);
        }

        private void OpenReadWrite()
        {
            Debug.Assert(this.databaseFileStream == null);
            Debug.Assert(this.br == null);
            Debug.Assert(this.bw == null);

            /* 
             * The lock on the database must be acquired prior to the  
             * database being opened to avoid dead lock
             */
            Lock();

            /*
             * Open the file for reading and writing. Other processes may not 
             * read or write to the file
             */
            this.databaseFileStream = File.Open(
                this.filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None
                );

            Debug.Assert(this.databaseFileStream.CanRead);
            Debug.Assert(this.databaseFileStream.CanSeek);
            Debug.Assert(this.databaseFileStream.CanWrite);

            this.br = new BinaryReader(this.databaseFileStream);
            this.bw = new BinaryWriter(this.databaseFileStream);
        }

        private void OpenCreate()
        {
            /* 
             * The lock on the database must be acquired prior to the  
             * database being opened to avoid dead lock
             */
            Lock();

            this.databaseFileStream = File.Create(this.filePath);
            this.br = new BinaryReader(this.databaseFileStream);
            this.bw = new BinaryWriter(this.databaseFileStream);
        }

        private bool IsLocked()
        {
            return File.Exists(GetLockFilename());
        }

        private string GetLockFilename()
        {
            return this.filePath + ".lock";
        }

        private string GetLockFileDirectory()
        {
            return Path.GetDirectoryName(Path.GetFullPath(this.filePath));
        }

        private void DeleteOrphanedLockFile()
        {
            try
            {
                using (FileStream fs = new FileStream(
                    GetLockFilename(), FileMode.Open, FileAccess.Write, FileShare.None, 512, FileOptions.DeleteOnClose
                    ))
                {
                    // The lock file has been orphaned, it will be deleted when closed
                    fs.Close();
                }
            }
            catch (Exception)
            {
                // The lock file hasn't been orphaned, so don't do anything
            }
        }

        private void WaitForUnlock()
        {
            using (FileSystemWatcher watcher = new FileSystemWatcher(GetLockFileDirectory(), GetLockFilename()))
            {
                while (true)
                {
                    if (IsLocked() != true)
                    {
                        return;
                    }

                    /* 
                     * Wait for the lock file to be deleted. If the lock file has 
                     * already been deleted then this call will wait indefinitely, 
                     * which is a bit odd. You would think that it would just drop 
                     * through.
                     */
                    WaitForChangedResult result = watcher.WaitForChanged(WatcherChangeTypes.Deleted, 10);
                    if (result.TimedOut != true)
                    {
                        // The lock file has been deleted, so the database is now unlocked
                        return;
                    }
                }
            }
        }
    }
}
