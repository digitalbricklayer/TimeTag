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
    /// Represents TimeTag errors that ocur during program execution.
    /// </summary>
    public class TimeTagException : Exception
    {
        /// <summary>
        /// Initialise a new TimeTagException
        /// </summary>
        /// <param name="msg">Message to attach to the exception</param>
        public TimeTagException(string msg)
            : base(msg)
        {
        }

        /// <summary>
        /// Initialise a new TimeTagException
        /// </summary>
        /// <param name="msg">Message to attach to the exception</param>
        /// <param name="innerException">Exception to attach to the exception</param>
        public TimeTagException(string msg, Exception innerException)
            : base(msg, innerException)
        {
        }
    }
}
