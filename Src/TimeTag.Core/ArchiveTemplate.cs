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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class ArchiveTemplate
    {
        /// <summary>
        /// Maximum length of the archive name
        /// </summary>
        public const int MaxNameLength = 56;

        /// <summary>
        /// Name of the archive. Must not exceed MaxNameLength in length
        /// </summary>
        private string name;

        /// <summary>
        /// Consolidation function used to create data points
        /// </summary>
        private ConsolidationFunctionType consolidationFunction;

        /// <summary>
        /// Minimum percentage of readings required in order to create a single 
        /// data point
        /// </summary>
        private int xFactor;

        /// <summary>
        /// Number of readings to be consolodated into a single data point
        /// </summary>
        private int numReadingsPerDataPoint;

        /// <summary>
        /// Maximum number of data points
        /// </summary>
        private int maxDataPoints;

        /// <summary>
        /// DAO for persisting the archive template to the database
        /// </summary>
        private IArchiveTemplateDao dao;

        /// <summary>
        /// Database in which the archive template is embedded
        /// </summary>
        private TimeSeriesDatabase database;

        // Sequence number of the archive
        private int sequenceNumber;

        /// <summary>
        /// Consolidation functions that the archive uses to create data points
        /// </summary>
        public enum ConsolidationFunctionType { Min, Max, Last, Average }

        /// <summary>
        /// Initialises an archive template
        /// </summary>
        /// <param name="archiveName">Name of the archive</param>
        /// <param name="type">Type of the archive can be one of: AVERAGE, MIN, MAX or LAST</param>
        /// <param name="xFactor">Percentage of readings that can be absent before a datum is considered invalid</param>
        /// <param name="readingsPerDataPoint">Number of readings that are processed to make a single datum</param>
        /// <param name="maxDataPoints">Maximum number of data points that can be stored in the archive</param>
        public ArchiveTemplate(string archiveName, ConsolidationFunctionType consolidationFunction, int xFactor, int readingsPerDataPoint, int numDataPointsPerArchive)
        {
            this.Name = archiveName;
            this.ConsolidationFunction = consolidationFunction;
            this.XFactor = xFactor;
            this.ReadingsPerDataPoint = readingsPerDataPoint;
            this.MaxDataPoints = numDataPointsPerArchive;
        }

        internal ArchiveTemplate(TimeSeriesDatabase database, ArchiveTemplateDto dto)
        {
            this.database = database;
            FixupFromDto(dto);
        }

        /// <summary>
        /// Gets the name of the archive
        /// </summary>
        public string Name
        {
            get { return this.name; }
            set { this.name = value; }
        }

        /// <summary>
        /// Gets the consolidation function type can be one of: AVERAGE, MAX, MIN, LAST
        /// </summary>
        public ConsolidationFunctionType ConsolidationFunction
        {
            get { return this.consolidationFunction; }
            set { this.consolidationFunction = value; }
        }

        /// <summary>
        /// Gets the percentage of readings that can be absent before a data point is considered invalid
        /// </summary>
        public int XFactor
        {
            get { return this.xFactor; }
            set { this.xFactor = value; }
        }

        /// <summary>
        /// Gets the number of readings that are processed to make a single data point
        /// </summary>
        public int ReadingsPerDataPoint
        {
            get { return this.numReadingsPerDataPoint; }
            set { this.numReadingsPerDataPoint = value; }
        }

        /// <summary>
        /// Gets the maximum number of data points that can be stored in the archive
        /// </summary>
        public int MaxDataPoints
        {
            get { return this.maxDataPoints; }
            set { this.maxDataPoints = value; }
        }

        /// <summary>
        /// Gets the archive template sequence number ie the order in which the 
        /// archive is persisted in the database
        /// </summary>
        internal int SequenceNumber
        {
            get { return this.sequenceNumber; }
            set { this.sequenceNumber = value; }
        }

        /// <summary>
        /// Create the archive in the database
        /// </summary>
        public void Create(TimeSeriesDatabase database)
        {
            Debug.Assert(this.dao == null);
            Debug.Assert(database != null);

            this.database = database;

            this.dao = this.database.GetDaoFactory().CreateArchiveTemplate(this.database.Dao);
            this.dao.Create(CreateDto());
        }

        /// <summary>
        /// Sets the archive template name. This changes the name of all archives created from 
        /// this template
        /// </summary>
        /// <param name="newTitle">New archive template name</param>
        public void SetName(string newName)
        {
            Debug.Assert(this.dao != null);

            if (newName == this.Name)
            {
                // The new name is the same as the existing one...
                return;
            }

            if (newName.Length > ArchiveTemplate.MaxNameLength)
            {
                // ERROR: the new archive name is too long
                throw new ArgumentException("Archive name is too long");
            }

            string oldName = this.Name;

            this.Name = newName;

            // Persist the new name
            this.dao.SetName(newName);

            // The data sources are index by name in the database
            this.database.RenameArchiveTemplate(oldName, newName);
        }

        /// <summary>
        /// Gets a user displayable string representation of the consolidation function
        /// </summary>
        internal string ConvertArchiveTypeToString()
        {
            string consolidationFunctionAsString = "";

            switch (this.consolidationFunction)
            {
                case ConsolidationFunctionType.Average:
                    consolidationFunctionAsString = "AVERAGE";
                    break;

                case ConsolidationFunctionType.Last:
                    consolidationFunctionAsString = "LAST";
                    break;

                case ConsolidationFunctionType.Max:
                    consolidationFunctionAsString = "MAX";
                    break;

                case ConsolidationFunctionType.Min:
                    consolidationFunctionAsString = "MIN";
                    break;

                default:
                    Debug.Assert(false, "ConvertArchiveTypeToString doesn't know about a type");
                    break;
            }

            return consolidationFunctionAsString;
        }

        internal ArchiveTemplateDto CreateDto()
        {
            ArchiveTemplateDto dto = new ArchiveTemplateDto();
            dto.Dao = this.dao;
            dto.Name = this.Name;
            dto.ConsolidationFunction = (int)this.ConsolidationFunction;
            dto.XFactor = this.XFactor;
            dto.ReadingsPerDataPoint = this.ReadingsPerDataPoint;
            dto.MaxDataPoints = this.MaxDataPoints;
            return dto;
        }

        private void FixupFromDto(ArchiveTemplateDto dto)
        {
            this.dao = dto.Dao;
            this.name = dto.Name;
            this.maxDataPoints = dto.MaxDataPoints;
            this.numReadingsPerDataPoint = dto.ReadingsPerDataPoint;
            this.consolidationFunction = (ConsolidationFunctionType)dto.ConsolidationFunction;
            this.xFactor = dto.XFactor;
        }

        /// <summary>
        /// Collection of archive templates
        /// </summary>
        public class ArchiveTemplateCollection : IEnumerable
        {
            private Dictionary<string, ArchiveTemplate> archives = new Dictionary<string, ArchiveTemplate>();

            internal ArchiveTemplateCollection()
            {
            }

            /// <summary>
            /// Add a new archive to the collection
            /// </summary>
            /// <param name="newArchiveTemplate">New archive to add</param>
            public void Add(ArchiveTemplate newArchiveTemplate)
            {
                this.archives.Add(newArchiveTemplate.Name, newArchiveTemplate);
            }

            /// <summary>
            /// Returns an enumerator for the collection
            /// </summary>
            /// <returns>Enumerator object</returns>
            public IEnumerator GetEnumerator()
            {
                return this.archives.Values.GetEnumerator();
            }

            /// <summary>
            /// Gets the number of archives in the collection
            /// </summary>
            public int Count
            {
                get { return this.archives.Count; }
            }

            /// <summary>
            /// Returns the archive template at the given index
            /// </summary>
            /// <param name="index">The index of the template</param>
            /// <returns>Archive template</returns>
            public ArchiveTemplate GetArchiveTemplateByIndex(int index)
            {
                int counter = 1;
                ArchiveTemplate foundArchiveTemplate = null;
                foreach (ArchiveTemplate archiveTemplate in this.archives.Values)
                {
                    if (counter++ == index)
                    {
                        foundArchiveTemplate = archiveTemplate;
                        break;
                    }
                }
                return foundArchiveTemplate;
            }

            /// <summary>
            /// Return the archive referenced by the given name
            /// </summary>
            /// <param name="archiveTemplateName">Name of the archive to find</param>
            /// <returns>Archive or null if no data source can be found with 
            /// the given name</returns>
            public ArchiveTemplate GetArchiveTemplateByName(string archiveTemplateName)
            {
                if (this.archives.ContainsKey(archiveTemplateName))
                {
                    return this.archives[archiveTemplateName];
                }

                // The archive template specified doesn't exist
                return null;
            }

            /// <summary>
            /// Clear the contents of the archive template collection
            /// </summary>
            public void Clear()
            {
                this.archives.Clear();
            }

            internal void RenameArchiveTemplate(string oldName, string newName)
            {
                ArchiveTemplate templateToRename = this.archives[oldName];
                this.archives.Remove(oldName);
                this.archives.Add(newName, templateToRename);
            }
        }
    }
}
