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

    internal class AverageConsolidationFunction : IConsolidationFunction
    {
        private Archive archive;

        public AverageConsolidationFunction(Archive archive)
        {
            this.archive = archive;
        }

        public DataPoint CreateAccumulatedDataPoint(Reading newReading)
        {
            return new DataPoint(CalcAverage(), newReading.Timestamp);
        }

        private double CalcAverage()
        {
            /*
             * The average should never be counted when there are no readings 
             * because the x factor threshold would not be met so a NaN value 
             * would be created
             */
            Debug.Assert(this.archive.AccumulatedReadings.Count > 0);

            /*
             * The average is the aggregate divided by the number of readings 
             * used to calculate the aggregate
             */
            return CalcAggregate() / this.archive.AccumulatedReadings.Count;
        }

        private double CalcAggregate()
        {
            double aggregate = 0D;

            // Calculate the aggregate of the readings received for this slot
            foreach (Reading r in this.archive.AccumulatedReadings)
            {
                aggregate += r.Value;
            }

            return aggregate;
        }
    }
}
