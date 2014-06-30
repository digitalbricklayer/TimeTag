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
    using System.Diagnostics;

    /// <summary>
    /// Threshold providing a minimum and maximum range of values
    /// </summary>
    public class Range
    {
        private IRangeDao dao;

        // Minimum value of the range
        private double min;

        // Maximum value of range
        private double max;

        /// <summary>
        /// Initialises a new range with a min and max value
        /// </summary>
        /// <param name="min">Minimum value of range</param>
        /// <param name="max">Maximum value of range</param>
        public Range(double min, double max)
        {
            this.min = min;
            this.max = max;
        }

        /// <summary>
        /// Default constructor used when de-serialising
        /// </summary>
        internal Range()
        {
        }

        public double Min
        {
            get { return this.min; }
            internal set { this.min = value; }
        }

        public double Max
        {
            get { return this.max; }
            internal set { this.max = value; }
        }
 
        internal IRangeDao Dao
        {
            get { return this.dao; }
        }

        public override bool Equals(Object obj)
        {
            if (obj == null) return false;

            Range t = obj as Range;

            if (t == null) return false;

            if (this.min.CompareTo(t.min) == 0 &&
                this.max.CompareTo(t.max) == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            throw new NotImplementedException();
        }

        public bool IsValid(double value)
        {
            return value >= this.min && value <= this.max;
        }

        internal RangeDto CreateDto()
        {
            RangeDto dto = new RangeDto();
            dto.Dao = this.dao;
            dto.Min = this.min;
            dto.Max = this.max;
            return dto;
        }

        internal void FixupFromDto(RangeDto dto)
        {
            this.dao = dto.Dao;
            this.min = dto.Min;
            this.max = dto.Max;
        }
    }
}
