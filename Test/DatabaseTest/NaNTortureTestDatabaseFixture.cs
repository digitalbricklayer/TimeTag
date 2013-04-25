namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using NUnit.Framework;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Fixture for testing a single data source with only the final reading being submitted
    /// </summary>
    [TestFixture]
    public class NaNTortureTestDatabaseFixture
    {
        /// <summary>
        /// Database filename
        /// </summary>
        private const string DatabaseFilename = "NaNTortureTestDatabaseFixture.ptd";

        /// <summary>
        /// Title of the database
        /// </summary>
        private const string DatabaseTitle = "A database to test the NaN functionality";

        /// <summary>
        /// Timestamp the database is due to start
        /// </summary>
        private DateTime startTime = new DateTime(2008, 5, 31, 23, 59, 0);

        /// <summary>
        /// Database to manipulate directly
        /// </summary>
        private TimeSeriesDatabase database;

        /// <summary>
        /// Database to validate the reading functionality
        /// </summary>
        private TimeSeriesDatabase freshDatabase;

        /*
         * The weird characters used to break the binary file persister, that's 
         * why they are being used here
         */
        private const string DataSourceName = "TRANEBJERG Ï¿½T";

        private const string TemperateDataSourceName = "Temperature";
        private const DataSource.ConversionFunctionType DataSourceType = DataSource.ConversionFunctionType.Gauge;

        private const string AllArchiveName = "All";
        private const ArchiveTemplate.ConsolidationFunctionType AllArchiveType = ArchiveTemplate.ConsolidationFunctionType.Average;
        private const int AllNumDataPointPerArchive = 7 * 24 * 60;      // Number of minutes in a week
        private const int AllReadingsPerDataPoint = 1;

        private const ArchiveTemplate.ConsolidationFunctionType MinArchiveType = ArchiveTemplate.ConsolidationFunctionType.Min;
        private const string MinArchiveName = "24hr Min";
        private const int MinNumDataPointPerArchive = 7;
        private const int MinReadingsPerDataPoint = 24 * 60;            // One day's worth

        private const ArchiveTemplate.ConsolidationFunctionType MaxArchiveType = ArchiveTemplate.ConsolidationFunctionType.Max;
        private const string MaxArchiveName = "24hr Max";
        private const int MaxNumDataPointPerArchive = 7;
        private const int MaxReadingsPerDataPoint = 24 * 60;            // One day's worth

        private const string AvgArchiveName = "24hr average";
        private const ArchiveTemplate.ConsolidationFunctionType AvgArchiveType = ArchiveTemplate.ConsolidationFunctionType.Average;
        private const int AvgNumDataPointPerArchive = 7;
        private const int AvgReadingsPerDataPoint = 24 * 60;            // One day's worth

        private const double MaxThreshold = 50D;
        private const double MinThreshold = 0D;

        private const int XFactor = 10;

        private TimeSpan PollingInterval = new TimeSpan(0, 1, 0);

        [SetUp]
        protected void Setup()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            template.StartTime = this.startTime;

            template.AddDataSource(
                DataSourceName, DataSourceType, PollingInterval, MinThreshold, MaxThreshold
                );

            // All readings
            template.AddArchive(
                AllArchiveName, AvgArchiveType, XFactor, AllReadingsPerDataPoint, AllNumDataPointPerArchive
                );

            // 24hr average
            template.AddArchive(
                AvgArchiveName, AvgArchiveType, XFactor, AvgReadingsPerDataPoint, AvgNumDataPointPerArchive
                );

            // 24hr lows
            template.AddArchive(
                MinArchiveName, MinArchiveType, XFactor, MinReadingsPerDataPoint, MinNumDataPointPerArchive
                );

            // 24hr highs
            template.AddArchive(
                MaxArchiveName, MaxArchiveType, XFactor, MaxReadingsPerDataPoint, MaxNumDataPointPerArchive
                );

            // Create the database to disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);

            // Push the final reading - will create lots of NaN dataPoint
            this.database.Push(DataSourceName, new Reading(24D, new DateTime(2008, 6, 7, 23, 59, 0)));

            this.database.Close();

            /*
             * Load the database so that we can check whether it has been 
             * persisted properly
             */
            this.freshDatabase = TimeSeriesDatabase.Read(
                DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadWrite
                );
        }

        [TearDown]
        protected void TearDown()
        {
            this.freshDatabase.Close();
            this.database.Delete();
        }

        [Test]
        public void TestDatabase()
        {
            ValidateDatabase(this.database);
            ValidateDatabase(this.freshDatabase);
        }

        private void ValidateDatabase(TimeSeriesDatabase databaseToTest)
        {
            Assert.That(databaseToTest.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(databaseToTest.StartTime, Is.EqualTo(startTime));
            Assert.That(databaseToTest.DataSources.Count, Is.EqualTo(1));
            ValidateDataSource(database.GetDataSourceByName(DataSourceName));
        }

        private void ValidateDataSource(DataSource dataSourceToTest)
        {
            Assert.That(dataSourceToTest.Archives.Count, Is.EqualTo(4));

            ValidateAllArchive(dataSourceToTest.GetArchiveByName(AllArchiveName));
            ValidateAvgArchive(dataSourceToTest.GetArchiveByName(AvgArchiveName));
            ValidateMaxArchive(dataSourceToTest.GetArchiveByName(MaxArchiveName));
            ValidateMinArchive(dataSourceToTest.GetArchiveByName(MinArchiveName));
        }

        private void ValidateAllArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.XFactor, Is.EqualTo(XFactor));
            Assert.That(archiveToTest.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(archiveToTest.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(AllNumDataPointPerArchive));
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(AllNumDataPointPerArchive));

            // First dataPoint will start at interval after database start time
            DateTime timestamp = startTime + PollingInterval;

            // Validate all but the final dataPoint
            for (int i = 1; i < archiveToTest.DataPoints.Count - 1; i++)
            {
                DataPoint d = new DataPoint(Double.NaN, timestamp);
                Assert.That(archiveToTest.DataPoints.GetAt(i).Equals(d), Is.True);
                timestamp += PollingInterval;
            }

            DataPoint finalValue = new DataPoint(24D, new DateTime(2008, 6, 7, 23, 59, 0));

            // Final dataPoint should be the same as the only reading pushed into the data source
            Assert.That(archiveToTest.DataPoints.GetAt(archiveToTest.DataPoints.Count), Is.EqualTo(finalValue));
        }

        private void ValidateAvgArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.XFactor, Is.EqualTo(XFactor));
            Assert.That(archiveToTest.ConsolidationFunction, Is.EqualTo(AvgArchiveType));
            Assert.That(archiveToTest.NumReadingsPerDataPoint, Is.EqualTo(AvgReadingsPerDataPoint));
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(AvgNumDataPointPerArchive));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(Double.NaN, new DateTime(2008, 6, 1, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 2, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 3, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 4, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 5, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 6, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 7, 23, 59, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(AvgNumDataPointPerArchive));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateMaxArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.XFactor, Is.EqualTo(XFactor));
            Assert.That(archiveToTest.ConsolidationFunction, Is.EqualTo(MaxArchiveType));
            Assert.That(archiveToTest.NumReadingsPerDataPoint, Is.EqualTo(MaxReadingsPerDataPoint));
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(MaxNumDataPointPerArchive));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(Double.NaN, new DateTime(2008, 6, 1, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 2, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 3, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 4, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 5, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 6, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 7, 23, 59, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(MaxNumDataPointPerArchive));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateMinArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.XFactor, Is.EqualTo(XFactor));
            Assert.That(archiveToTest.ConsolidationFunction, Is.EqualTo(MinArchiveType));
            Assert.That(archiveToTest.NumReadingsPerDataPoint, Is.EqualTo(MinReadingsPerDataPoint));
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(MinNumDataPointPerArchive));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(Double.NaN, new DateTime(2008, 6, 1, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 2, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 3, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 4, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 5, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 6, 23, 59, 0)),
                new DataPoint(Double.NaN, new DateTime(2008, 6, 7, 23, 59, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(MinNumDataPointPerArchive));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }
    }
}
