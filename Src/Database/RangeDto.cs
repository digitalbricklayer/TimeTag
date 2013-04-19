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

    /// <summary>
    /// Data transfer object of the Threshold class
    /// </summary>
    internal class RangeDto
    {
        private IRangeDao dao;
        private double min;
        private double max;

        /// <summary>
        /// Data access object for the DTO
        /// </summary>
        public IRangeDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Minimum threshold
        /// </summary>
        public double Min
        {
            get { return this.min; }
            set { this.min = value; }
        }

        /// <summary>
        /// Maximum threshold
        /// </summary>
        public double Max
        {
            get { return this.max; }
            set { this.max = value; }
        }
    }
}
