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

using TimeTag.Core.Binary;

namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Diagnostics;
    using System.IO;

    public sealed class TimeSeriesDatabase
    {
        /// <summary>
        /// Maximum length of the database title
        /// </summary>
        public const int MaxTitleLength = 56;

        /// <summary>
        /// A connection to the database can either be read only or read and write
        /// </summary>
        public enum ConnectionMode { Disconnected, ReadOnly, ReadWrite }

        // Database title
        private string title = "";

        // Full path to the database
        private string filePath;

        // Timestamp when the database was started
        private DateTime startTime;

        // Collection of data sources indexed by name
        private ArchiveTemplate.ArchiveTemplateCollection archiveTemplates = new ArchiveTemplate.ArchiveTemplateCollection();

        // Collection of data sources
        private DataSource.DataSourceCollection dataSources = new DataSource.DataSourceCollection();

        /*
         * Database data access object. All read and writes to the database go 
         * through the DAO.
         */
        private ITimeSeriesDatabaseDao dao;

        // The current DAO factory
        private IDaoFactory daoFactory;

        // The connection mode for the database
        private ConnectionMode mode;

        /// <summary>
        /// Initialise a new TimeTag database object without creating the database 
        /// itself on disk.
        /// </summary>
        /// <param name="filePath">Full path to the database</param>
        private TimeSeriesDatabase(string filePath)
        {
            this.filePath = filePath;
        }

        /// <summary>
        /// Get the collection of archive templates in the database
        /// </summary>
        public ArchiveTemplate.ArchiveTemplateCollection ArchiveTemplates
        {
            get { return this.archiveTemplates; }
        }

        /// <summary>
        /// Get the collection of data sources in the database
        /// </summary>
        public DataSource.DataSourceCollection DataSources
        {
            get { return this.dataSources; }
        }

        /// <summary>
        /// Get the full path of the database
        /// </summary>
        public string FilePath
        {
            get { return this.filePath; }
        }

        /// <summary>
        /// Gets the timestamp the database started to receive data
        /// </summary>
        public DateTime StartTime
        {
            get { return this.startTime; }
            private set { this.startTime = value; }
        }

        /// <summary>
        /// Gets the database title
        /// </summary>
        public string Title
        {
            get { return this.title; }
            private set { this.title = value; }
        }

        /// <summary>
        /// Gets the database statistics
        /// </summary>
        public DatabaseStats Stats
        {
            get { return AggregateStats(); }
        }

        internal ITimeSeriesDatabaseDao Dao
        {
            get { return this.dao; }
        }

        /// <summary>
        /// Create the outline database on disk
        /// </summary>
        /// <param name="filePath">File name for the database</param>
        /// <param name="template">Database template</param>
        public static TimeSeriesDatabase Create(string filePath, TimeSeriesDatabaseTemplate template)
        {
            TimeSeriesDatabase newDatabase = new TimeSeriesDatabase(filePath);
            newDatabase.Create(template);
            return newDatabase;
        }

        /// <summary>
        /// Read an existing database
        /// </summary>
        /// <param name="databaseFile">Path to the database</param>
        /// <returns>A time series database</returns>
        public static TimeSeriesDatabase Read(string databaseFile)
        {
            return Read(databaseFile, ConnectionMode.ReadOnly);
        }

        /// <summary>
        /// Read an existing database
        /// </summary>
        /// <param name="databaseFile">Path to the database</param>
        /// <returns>A time series database</returns>
        public static TimeSeriesDatabase Read(string databaseFile, ConnectionMode mode)
        {
            TimeSeriesDatabase databaseToRead = new TimeSeriesDatabase(databaseFile);
            databaseToRead.Read(mode);
            return databaseToRead;
        }

        /// <summary>
        /// Read the database
        /// </summary>
        public void Read()
        {
            Read(ConnectionMode.ReadOnly);
        }

        /// <summary>
        /// Read the database with the given mode
        /// </summary>
        /// <param name="mode">Mode the database will be opened in</param>
        public void Read(ConnectionMode mode)
        {
            this.mode = mode;

            // Make sure the database status and connection mode are compatible
            if (ValidateDatabaseMode() != true)
            {
                throw new TimeTagException("Connection mode is incompatible with a read only file");
            }

            if (this.dao == null)
            {
                this.dao = GetDaoFactory().CreateTimeSeriesDatabase(this.FilePath);
            }

            // Reset the existing database contents
            Reset();

            try
            {
                // Open a connection to the database
                this.dao.Connect(mode);
            }
            catch (IOException e)
            {
                throw new TimeTagException("Database is locked", e);
            }

            // Read everything in from the database
            TimeSeriesDatabaseDto dto = this.dao.Read();

            // Build the database from the DTO
            FixupFromDto(dto);
        }

        /// <summary>
        /// Permanently delete the database from the disk
        /// </summary>
        public void Delete()
        {
            if (this.dao != null)
            {
                if (this.dao.IsConnected())
                {
                    this.dao.Close();
                    this.dao = null;
                }
            }

            File.Delete(this.filePath);
        }

        /// <summary>
        /// Close the database connection opened by either Read or Create
        /// </summary>
        public void Close()
        {
            this.mode = ConnectionMode.Disconnected;

            if (this.dao != null)
            {
                this.dao.Close();
                this.dao = null;
            }
        }

        /// <summary>
        /// Push a collection of readings into the specified data source
        /// </summary>
        /// <param name="dataSourceName">Data source into which the reading should be pushed</param>
        /// <param name="newReadings">New readings to be pushed into the database</param>
        public void Push(string dataSourceName, Reading[] newReadings)
        {
            if (GetDataSourceByName(dataSourceName) == null)
            {
                // ERROR: the data source specified doesn't exist
                throw new ArgumentException(
                    string.Format("Unable to find data source: {0}", dataSourceName));
            }

            Push(Payload.MakePayload(GetDataSourceByName(dataSourceName), newReadings));
        }

        /// <summary>
        /// Push the new reading into the specified data source
        /// </summary>
        /// <param name="dataSourceName">Data source into which the reading should be pushed</param>
        /// <param name="newReading">New reading to be pushed into the database</param>
        public void Push(string dataSourceName, Reading newReading)
        {
            Reading[] reading = { newReading };
            Push(dataSourceName, reading);
        }

        /// <summary>
        /// Push a collection of payloads into the database
        /// </summary>
        /// <param name="dataSourceName">Data source into which the reading should be pushed</param>
        /// <param name="newReadings">New readings to be pushed into the database</param>
        public void Push(Payload[] newPayloads)
        {
            if (this.mode == ConnectionMode.ReadOnly || this.mode == ConnectionMode.Disconnected)
            {
                // ERROR: the connection mode is read only, you can't change the database
                throw new TimeTagException(
                    "Database is unable accept new readings (may be read-only or not connected)"
                    );
            }

            foreach (Payload payload in newPayloads)
            {
                if (GetDataSourceByName(payload.DataSource.Name) == null)
                {
                    // ERROR: the data source specified doesn't exist
                    throw new ArgumentException(
                        string.Format("Unable to find data source: {0}", payload.DataSource.Name));
                }

                GetDataSourceByName(payload.DataSource.Name).Push(payload.Readings);
            }
        }

        /// <summary>
        /// Push a payload into the database
        /// </summary>
        /// <param name="dataSourceName">Data source into which the reading should be pushed</param>
        /// <param name="newReadings">New readings to be pushed into the database</param>
        public void Push(Payload newPayload)
        {
            Payload[] payloads = { newPayload };
            Push(payloads);
        }

        /// <summary>
        /// Return the data source with the given name
        /// </summary>
        /// <param name="dataSourceName">Name of the data source to find</param>
        /// <returns>Data source</returns>
        public DataSource GetDataSourceByName(string dataSourceName)
        {
            return this.dataSources.GetDataSourceByName(dataSourceName);
        }

        /// <summary>
        /// Returns the archive template with the given name
        /// </summary>
        /// <param name="archiveName">Name of the archive template to find</param>
        /// <returns>Archive Template</returns>
        public ArchiveTemplate GetArchiveTemplateByName(string archiveName)
        {
            return this.archiveTemplates.GetArchiveTemplateByName(archiveName);
        }

        /// <summary>
        /// Import the database state from a previously exported XML file
        /// </summary>
        /// <param name="importFile">File name to import from</param>
        public static TimeSeriesDatabase Import(string databaseFile, string importFile)
        {
            XmlFileTimeSeriesDatabaseDao xmlImporter = new XmlFileTimeSeriesDatabaseDao(importFile);

            try
            {
                TimeSeriesDatabaseDto dto = xmlImporter.Read();

                TimeSeriesDatabase newDatabase = new TimeSeriesDatabase(databaseFile);

                // Create the database from the DTO
                try
                {
                    newDatabase.Create(dto);
                }
                catch (IOException e)
                {
                    throw new TimeTagException(e.Message, e);
                }

                return newDatabase;
            }
            catch (System.Xml.XmlException e)
            {
                throw new TimeTagException("Badly formed XML import document", e);
            }
        }

        /// <summary>
        /// Export the database state to a XML file
        /// </summary>
        /// <param name="importFile">File name to export to</param>
        public void Export(string exportFile)
        {
            XmlFileTimeSeriesDatabaseDao xmlExporter = new XmlFileTimeSeriesDatabaseDao(exportFile);
            xmlExporter.Create(CreateDto());
        }

        /// <summary>
        /// Sets the database title
        /// </summary>
        /// <param name="newTitle">New database title</param>
        public void SetTitle(string newTitle)
        {
            Debug.Assert(this.dao != null);

            if (newTitle.Length > TimeSeriesDatabase.MaxTitleLength)
            {
                // ERROR: The new title is too large
                throw new ArgumentException(
                    string.Format("New title is {1} characters long. It must not exceed {0} characters in length", 
                        TimeSeriesDatabase.MaxTitleLength, newTitle.Length));
            }

            this.dao.SetTitle(newTitle);
        }

        internal IDaoFactory GetDaoFactory()
        {
            if (this.daoFactory == null)
            {
                this.daoFactory = CreateDaoFactory();
            }
            return this.daoFactory;
        }

        internal ArchiveTemplate GetArchiveTemplateByIndex(int sequenceNumber)
        {
            return this.archiveTemplates.GetArchiveTemplateByIndex(sequenceNumber);
        }

        internal void RenameDataSource(string oldDataSourceName, string newDataSourceName)
        {
            this.dataSources.RenameDataSource(oldDataSourceName, newDataSourceName);
        }

        internal void RenameArchiveTemplate(string oldName, string newName)
        {
            this.archiveTemplates.RenameArchiveTemplate(oldName, newName);
        }

        /// <summary>
        /// Create the outline database on disk
        /// </summary>
        /// <param name="template">Database template</param>
        private void Create(TimeSeriesDatabaseTemplate template)
        {
            this.mode = ConnectionMode.ReadWrite;
            CreateFromTemplate(template);
            Create();
        }

        private void Reset()
        {
            this.dataSources.Clear();
            this.archiveTemplates.Clear();
        }

        private void Create()
        {
            this.CreateDatabase(CreateDto());
        }

        private void Create(TimeSeriesDatabaseDto dto)
        {
            this.FixupFromDto(dto);
            this.CreateDatabase(dto);
        }

        private void CreateDatabase(TimeSeriesDatabaseDto dto)
        {
            this.dao = GetDaoFactory().CreateTimeSeriesDatabase(this.FilePath);
            try
            {
                this.dao.Create(dto);

                foreach (ArchiveTemplate archiveTemplate in this.archiveTemplates)
                {
                    archiveTemplate.Create(this);
                }

                foreach (DataSource dataSource in this.dataSources)
                {
                    dataSource.Create();
                }
            }
            catch (IOException e)
            {
                throw new TimeTagException(e.Message, e);
            }
        }

        private DataSource AddDataSource(DataSourceTemplate template)
        {
            DataSource newDataSource = CreateDataSourceFromTemplate(template);
            newDataSource.AddArchives(this.archiveTemplates);
            this.dataSources.Add(newDataSource);
            return newDataSource;
        }

        private DataSource CreateDataSourceFromTemplate(DataSourceTemplate template)
        {
            return new DataSource(this, template);
        }

        private DataSource CreateDataSourceFromDto(DataSourceDto dto)
        {
            return new DataSource(this, dto);
        }

        private void FixupFromDto(TimeSeriesDatabaseDto dto)
        {
            this.Title = dto.Title;
            this.StartTime = dto.StartTime;
            FixupArchiveTemplatesFromDto(dto);
            FixupDataSourcesFromDto(dto);
        }

        private void FixupArchiveTemplatesFromDto(TimeSeriesDatabaseDto dto)
        {
            foreach (ArchiveTemplateDto archiveTemplateDto in dto.ArchiveTemplates)
            {
                ArchiveTemplate newArchiveTemplate = new ArchiveTemplate(this, archiveTemplateDto);
                this.archiveTemplates.Add(newArchiveTemplate);
            }
        }

        private void FixupDataSourcesFromDto(TimeSeriesDatabaseDto dto)
        {
            foreach (DataSourceDto dataSourceDto in dto.DataSources)
            {
                DataSource newDataSource = this.CreateDataSourceFromDto(dataSourceDto);
                this.dataSources.Add(newDataSource);
            }
        }

        private TimeSeriesDatabaseDto CreateDto()
        {
            TimeSeriesDatabaseDto dto = new TimeSeriesDatabaseDto();
            dto.Title = this.Title;
            dto.StartTime = this.StartTime;
            dto.Dao = this.dao;

            foreach (ArchiveTemplate archiveTemplate in this.ArchiveTemplates)
            {
                dto.AddArchiveTemplate(archiveTemplate.CreateDto());
            }

            foreach (DataSource ds in this.DataSources)
            {
                dto.AddDataSource(ds.CreateDto());
            }

            return dto;
        }

        private void CreateFromTemplate(TimeSeriesDatabaseTemplate template)
        {
            this.Title = template.Title;
            this.StartTime = template.StartTime;
            CreateArchivesFromTemplate(template);
            CreateDataSourcesFromTemplate(template);
        }

        private void CreateArchivesFromTemplate(TimeSeriesDatabaseTemplate template)
        {
            foreach (ArchiveTemplate archiveTemplate in template.Archives)
            {
                this.archiveTemplates.Add(archiveTemplate);
            }
        }

        private void CreateDataSourcesFromTemplate(TimeSeriesDatabaseTemplate template)
        {
            foreach (DataSourceTemplate dataSourceTemplate in template.DataSources)
            {
                AddDataSource(dataSourceTemplate);
            }
        }

        private IDaoFactory CreateDaoFactory()
        {
            /*
             * Default to the binary file factory for now...will require some 
             * kind of connection string in due course
             */
            return new BinaryFileDaoFactory();
        }

        private DatabaseStats AggregateStats()
        {
            DatabaseStats stats = new DatabaseStats();
            foreach (DataSource dataSource in this.dataSources)
            {
                stats.Add(dataSource.Stats);
            }
            stats.CalcTotals();
            return stats;
        }

        private bool ValidateDatabaseMode()
        {
            // A read only file with a connection mode of ReadWrite is not valid
            if (this.mode == ConnectionMode.ReadWrite)
            {
                if ((File.GetAttributes(this.filePath) & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
