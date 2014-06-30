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

    internal class LastConsolidationFunction : SelectiveConsolidationFunction
    {
        public LastConsolidationFunction(Archive archive)
            : base(archive)
        {
        }

        protected override bool IsSelected(Reading newReading)
        {
            /*
             * The last archive stores the last reading before the datum is 
             * created, so the new reading should always be saved. When the
             * slot is completed the newest reading will then become the
             * datum for the whole slot.
             */
            return true;
        }
    }
}
