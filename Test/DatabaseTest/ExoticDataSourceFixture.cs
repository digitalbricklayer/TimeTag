namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using NUnit.Framework;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Fixture for testing all data source conversion functions
    /// </summary>
    [TestFixture]
    public class ExoticDataSourceFixture
    {
        private static readonly string DatabaseFilename = "ExoticDataSourceFixture.ptd";
        private static readonly string DatabaseTitle = "Fixture for testing all data source conversion functions";
        private static readonly string GaugeDataSourceName = "Gauge";
        private static readonly string AbsoluteDataSourceName = "Absolute";
        private static readonly string CounterDataSourceName = "Counter";
        private static readonly string DeriveDataSourceName = "Derive";
        private static readonly DateTime StartTime = new DateTime(2005, 1, 1, 0, 0, 0);
        private TimeSeriesDatabase database;
        private TimeSeriesDatabase freshDatabase;

        [SetUp]
        protected void Setup()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            template.StartTime = StartTime;

            template.AddDataSource(
                GaugeDataSourceName, DataSource.ConversionFunctionType.Gauge, new TimeSpan(0, 5, 0), 0D, 10000D);
            template.AddDataSource(
                AbsoluteDataSourceName, DataSource.ConversionFunctionType.Absolute, new TimeSpan(0, 5, 0), 0D, 10000D);
            template.AddDataSource(
                CounterDataSourceName, DataSource.ConversionFunctionType.Counter, new TimeSpan(0, 5, 0), 0D, 10000D);
            template.AddDataSource(
                DeriveDataSourceName, DataSource.ConversionFunctionType.Derive, new TimeSpan(0, 5, 0), 0D, 10000D);

            template.AddArchive("Average", ArchiveTemplate.ConsolidationFunctionType.Average, 50, 1, 10);

            // Create the database on disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);

            Reading[] readings = 
            {
                new Reading(300D, new DateTime(2005, 1, 1, 0, 5, 0)),
                new Reading(600D, new DateTime(2005, 1, 1, 0, 10, 0)),
                new Reading(900D, new DateTime(2005, 1, 1, 0, 15, 0)),
                new Reading(1200D, new DateTime(2005, 1, 1, 0, 20, 0)),
                new Reading(1500D, new DateTime(2005, 1, 1, 0, 25, 0)),
                new Reading(1800D, new DateTime(2005, 1, 1, 0, 30, 0)),
                new Reading(2100D, new DateTime(2005, 1, 1, 0, 35, 0)),
                new Reading(2400D, new DateTime(2005, 1, 1, 0, 40, 0)),
                new Reading(2700D, new DateTime(2005, 1, 1, 0, 45, 0)),
                new Reading(3000D, new DateTime(2005, 1, 1, 0, 50, 0))
            };

            this.database.Push(GaugeDataSourceName, readings);
            this.database.Push(AbsoluteDataSourceName, readings);
            this.database.Push(DeriveDataSourceName, readings);
            this.database.Push(CounterDataSourceName, readings);

            this.database.Close();

            /*
             * Load the database so that we can check whether it has been 
             * persisted properly
             */
            this.freshDatabase = TimeSeriesDatabase.Read(
                DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadWrite);
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
            Assert.That(databaseToTest.StartTime, Is.EqualTo(StartTime));
            Assert.That(databaseToTest.DataSources.Count, Is.EqualTo(4));

            ValidateGaugeDataSource(databaseToTest.GetDataSourceByName(GaugeDataSourceName));
            ValidateAbsoluteDataSource(databaseToTest.GetDataSourceByName(AbsoluteDataSourceName));
            ValidateDeriveDataSource(databaseToTest.GetDataSourceByName(DeriveDataSourceName));
            ValidateCounterDataSource(databaseToTest.GetDataSourceByName(CounterDataSourceName));
        }

        private void ValidateGaugeDataSource(DataSource dataSourceToTest)
        {
            ValidateGaugeArchive(dataSourceToTest.GetArchiveByName("Average"));
        }

        private void ValidateAbsoluteDataSource(DataSource dataSourceToTest)
        {
            ValidateAbsoluteArchive(dataSourceToTest.GetArchiveByName("Average"));
        }

        private void ValidateCounterDataSource(DataSource dataSourceToTest)
        {
            ValidateCounterArchive(dataSourceToTest.GetArchiveByName("Average"));
        }

        private void ValidateDeriveDataSource(DataSource dataSourceToTest)
        {
            ValidateDeriveArchive(dataSourceToTest.GetArchiveByName("Average"));
        }

        private void ValidateGaugeArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(10));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(300D,     new DateTime(2005, 1, 1, 0, 5, 0)),
                new DataPoint(600D,     new DateTime(2005, 1, 1, 0, 10, 0)),
                new DataPoint(900D,     new DateTime(2005, 1, 1, 0, 15, 0)),
                new DataPoint(1200D,    new DateTime(2005, 1, 1, 0, 20, 0)),
                new DataPoint(1500D,    new DateTime(2005, 1, 1, 0, 25, 0)),
                new DataPoint(1800D,    new DateTime(2005, 1, 1, 0, 30, 0)),
                new DataPoint(2100D,    new DateTime(2005, 1, 1, 0, 35, 0)),
                new DataPoint(2400D,    new DateTime(2005, 1, 1, 0, 40, 0)),
                new DataPoint(2700D,    new DateTime(2005, 1, 1, 0, 45, 0)),
                new DataPoint(3000D,    new DateTime(2005, 1, 1, 0, 50, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateAbsoluteArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(10));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(1D,   new DateTime(2005, 1, 1, 0, 5, 0)),
                new DataPoint(2D,   new DateTime(2005, 1, 1, 0, 10, 0)),
                new DataPoint(3D,   new DateTime(2005, 1, 1, 0, 15, 0)),
                new DataPoint(4D,   new DateTime(2005, 1, 1, 0, 20, 0)),
                new DataPoint(5D,   new DateTime(2005, 1, 1, 0, 25, 0)),
                new DataPoint(6D,   new DateTime(2005, 1, 1, 0, 30, 0)),
                new DataPoint(7D,   new DateTime(2005, 1, 1, 0, 35, 0)),
                new DataPoint(8D,   new DateTime(2005, 1, 1, 0, 40, 0)),
                new DataPoint(9D,   new DateTime(2005, 1, 1, 0, 45, 0)),
                new DataPoint(10D,  new DateTime(2005, 1, 1, 0, 50, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateCounterArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(10));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(Double.NaN,   new DateTime(2005, 1, 1, 0, 5, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 10, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 15, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 20, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 25, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 30, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 35, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 40, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 45, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 50, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateDeriveArchive(Archive archiveToTest)
        {
            Assert.That(archiveToTest.MaxDataPoints, Is.EqualTo(10));

            // Validate accumulated reading collection
            Assert.That(archiveToTest.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(Double.NaN,   new DateTime(2005, 1, 1, 0, 5, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 10, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 15, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 20, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 25, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 30, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 35, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 40, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 45, 0)),
                new DataPoint(1D,           new DateTime(2005, 1, 1, 0, 50, 0))
            };

            Assert.That(archiveToTest.DataPoints.Count, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(archiveToTest.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }
    }
}
