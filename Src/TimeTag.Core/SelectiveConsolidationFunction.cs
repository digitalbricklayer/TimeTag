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
    /// Abstract class implements the concept of an archive that selects a 
    /// single reading to be converted into a data point
    /// </summary>
    internal abstract class SelectiveConsolidationFunction : IConsolidationFunction
    {
        private Archive archive;

        // Currently selected reading. Only set when GetAccumulatedDataPoint is being executed
        private Reading selectedReading;

        public SelectiveConsolidationFunction(Archive archive)
        {
            this.archive = archive;
        }

        /// <summary>
        /// Get the curently selected reading
        /// </summary>
        protected Reading SelectedReading
        {
            get { return this.selectedReading; }
        }

        public DataPoint CreateAccumulatedDataPoint(Reading newReading)
        {
            // Find the selected reading
            foreach (Reading r in this.archive.AccumulatedReadings)
            {
                if (this.selectedReading == null)
                {
                    this.selectedReading = r;
                }
                else if (IsSelected(r))
                {
                    this.selectedReading = r;
                }
            }

            DataPoint accumulatedDataPoint = selectedReading.ConvertToDataPoint();
            // Selected reading must be null for the next invocation of this function
            this.selectedReading = null;
            return accumulatedDataPoint;
        }

        // Override for concrete selective archives
        protected abstract bool IsSelected(Reading newReading);
    }
}
