namespace Openxtra.TimeTag.Database
{
    using System;

    public sealed class Payload
    {
        private DataSource dataSource;
        private Reading[] readings;

        public static Payload MakePayload(DataSource dataSource, Reading newReading)
        {
            Reading[] reading = { newReading };
            return new Payload(dataSource, reading);
        }

        public static Payload MakePayload(DataSource dataSource, Reading[] newReadings)
        {
            return new Payload(dataSource, newReadings);
        }

        public DataSource DataSource
        {
            get { return this.dataSource; }
        }

        public Reading[] Readings
        {
            get { return this.readings; }
        }

        public int Size
        {
            get { return this.readings.Length; }
        }

        private Payload(DataSource dataSource, Reading[] readings)
        {
            this.dataSource = dataSource;
            this.readings = readings;
        }
    }
}
