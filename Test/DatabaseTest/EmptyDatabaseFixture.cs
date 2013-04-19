namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using NUnit.Framework;
    using NUnit.Framework.SyntaxHelpers;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Blank fixture for testing functionality of an empty database
    /// </summary>
    [TestFixture]
    public class EmptyDatabaseFixture
    {
        private const string DatabaseFilename = "Empty.ptd";
        private const string DatabaseTitle = "An empty database to test the empty boundary condition";
        private DateTime startTime = new DateTime(2005, 1, 1, 0, 0, 0);
        private TimeSeriesDatabase database;
        private TimeSeriesDatabase freshDatabase;

        [SetUp]
        protected void Setup()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            template.StartTime = startTime;

            template.AddDataSource(
                "Gauge", DataSource.ConversionFunctionType.Gauge, new TimeSpan(0, 0, 1), 10, 100);
            template.AddDataSource(
                "Counter", DataSource.ConversionFunctionType.Counter, new TimeSpan(0, 0, 1), 10, 100);
            template.AddDataSource(
                "Derive", DataSource.ConversionFunctionType.Derive, new TimeSpan(0, 0, 1), 10, 100);
            template.AddDataSource(
                "Absolute", DataSource.ConversionFunctionType.Absolute, new TimeSpan(0, 0, 1), 10, 100);

            template.AddArchive("Average", ArchiveTemplate.ConsolidationFunctionType.Average, 50, 30, 1000);
            template.AddArchive("Max", ArchiveTemplate.ConsolidationFunctionType.Max, 50, 30, 1000);
            template.AddArchive("Min", ArchiveTemplate.ConsolidationFunctionType.Min, 50, 30, 1000);
            template.AddArchive("Last", ArchiveTemplate.ConsolidationFunctionType.Last, 50, 30, 1000);

            // Create the database on disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);

            this.database.Close();

            // Load the database so that we can check whether it has been 
            // persisted properly
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
        public void TestPush()
        {
            this.freshDatabase.Push("Gauge", new Reading(10D, new DateTime(2005, 1, 1, 0, 1, 0)));
        }

        [Test]
        public void TestDatabase()
        {
            ValidateEmptyDatabase(this.database);
            ValidateEmptyDatabase(this.freshDatabase);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidDataSourcePush()
        {
            // Push a reading into a data source that doesn't exist
            this.freshDatabase.Push("Some junk", new Reading(2D, DateTime.Now));
        }

        private void ValidateEmptyDatabase(TimeSeriesDatabase databaseToTest)
        {
            Assert.That(databaseToTest.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(databaseToTest.StartTime, Is.EqualTo(startTime));
            Assert.That(databaseToTest.DataSources.Count, Is.EqualTo(4));
            ValidateEmptyDataSource(databaseToTest.GetDataSourceByName("Gauge"));
        }

        private static void ValidateEmptyDataSource(DataSource dataSourceToTest)
        {
            Assert.That(dataSourceToTest.LastReading, Is.Null);
            Assert.That(dataSourceToTest.GetArchiveByName("Average").MaxDataPoints, Is.EqualTo(1000));
            Assert.That(dataSourceToTest.GetArchiveByName("Average").DataPoints.Count, Is.EqualTo(0));
            Assert.That(dataSourceToTest.GetArchiveByName("Min").MaxDataPoints, Is.EqualTo(1000));
            Assert.That(dataSourceToTest.GetArchiveByName("Min").DataPoints.Count, Is.EqualTo(0));
            Assert.That(dataSourceToTest.GetArchiveByName("Max").MaxDataPoints, Is.EqualTo(1000));
            Assert.That(dataSourceToTest.GetArchiveByName("Max").DataPoints.Count, Is.EqualTo(0));
            Assert.That(dataSourceToTest.GetArchiveByName("Last").MaxDataPoints, Is.EqualTo(1000));
            Assert.That(dataSourceToTest.GetArchiveByName("Last").DataPoints.Count, Is.EqualTo(0));
        }
    }
}
