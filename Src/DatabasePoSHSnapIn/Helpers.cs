namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;

    internal static class DateTimeHelper
    {
        /// <summary>
        /// Convert a string representation of a time to a DateTime object.
        /// </summary>
        /// <param name="time">The time to convert.</param>
        /// <returns>DateTime object representing the converted time.</returns>
        public static DateTime CreateDateTimeFromString(string time)
        {
            DateTime result = DateTime.Now;

            StringComparer comparer = StringComparer.Create(
                System.Globalization.CultureInfo.CurrentCulture, true);

            if (comparer.Compare(time, "Now") != 0)
            {
                result = DateTime.Parse(time);
            }

            return result;
        }
    }
}