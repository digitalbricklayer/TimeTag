namespace Openxtra.TimeTag.Database
{
    using System;

    interface IConsolidationFunction
    {
        DataPoint CreateAccumulatedDataPoint(Reading newReading);
    }
}
