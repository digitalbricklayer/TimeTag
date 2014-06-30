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
    /// Data Transfer Object for the archive template
    /// </summary>
    internal class ArchiveTemplateDto
    {
        private IArchiveTemplateDao dao;
        private string name;
        private int consolidationFunction;
        private int xFactor;
        private int readingsPerDataPoint;
        private int maxDataPoints;

        /// <summary>
        /// Get the Data access object
        /// </summary>
        public IArchiveTemplateDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the archive name
        /// </summary>
        public string Name
        {
            get { return this.name; }
            set { this.name = value; }
        }

        /// <summary>
        /// Gets the archive type
        /// </summary>
        public int ConsolidationFunction
        {
            get { return this.consolidationFunction; }
            set { this.consolidationFunction = value; }
        }

        /// <summary>
        /// Gets the percentage of readings permitted to be unknown before the reading 
        /// is considered unknown
        /// </summary>
        public int XFactor
        {
            get { return this.xFactor; }
            set { this.xFactor = value; }
        }

        /// <summary>
        /// Gets the number of readings per datum
        /// </summary>
        public int ReadingsPerDataPoint
        {
            get { return this.readingsPerDataPoint; }
            set { this.readingsPerDataPoint = value; }
        }

        /// <summary>
        /// Gets the maximum data points that can be stored in the archive
        /// </summary>
        public int MaxDataPoints
        {
            get { return this.maxDataPoints; }
            set { this.maxDataPoints = value; }
        }
    }
}
