namespace Openxtra.TimeTag.Database
{
    using System;

    interface IConversionFunction
    {
        /// <summary>
        /// Pre-process the reading before it is stored
        /// </summary>
        /// <param name="newReading">Raw reading</param>
        /// <returns>Processed reading</returns>
        Reading PreProcessReading(Reading newReading);
    }
}
