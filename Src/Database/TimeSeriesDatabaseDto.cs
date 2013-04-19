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

namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Data Transfer Object for a time series database
    /// </summary>
    internal class TimeSeriesDatabaseDto
    {
        // Data access object associated with this DTO
        private ITimeSeriesDatabaseDao dao;

        // Title
        private string title = "";

        // Scheduled start time
        private DateTime startTime;

        // Templates for the archives that will be contained in the database
        private List<ArchiveTemplateDto> archiveTemplates = new List<ArchiveTemplateDto>();

        // Data sources contained in the database
        private List<DataSourceDto> dataSources = new List<DataSourceDto>();

        /// <summary>
        /// Gets the data access object used to persist the database. May be null if 
        /// the database hasn't been persisted
        /// </summary>
        public ITimeSeriesDatabaseDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the title of the database
        /// </summary>
        public string Title
        {
            get { return this.title; }
            set { this.title = value; }
        }

        /// <summary>
        /// Gets the database start time
        /// </summary>
        public DateTime StartTime
        {
            get { return this.startTime; }
            set { this.startTime = value; }
        }

        /// <summary>
        /// Data sources contained in the database
        /// </summary>
        public DataSourceDto[] DataSources
        {
            get { return this.dataSources.ToArray(); }
            set
            {
                this.dataSources.Clear();
                this.dataSources.AddRange(value);
            }
        }

        public ArchiveTemplateDto[] ArchiveTemplates
        {
            get { return this.archiveTemplates.ToArray(); }
            set
            {
                this.archiveTemplates.Clear();
                this.archiveTemplates.AddRange(value);
            }
        }

        public void AddDataSource(DataSourceDto newDataSource)
        {
            this.dataSources.Add(newDataSource);
        }

        public void AddArchiveTemplate(ArchiveTemplateDto newArchiveTemplate)
        {
            this.archiveTemplates.Add(newArchiveTemplate);
        }
    }
}
