namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using NUnit.Framework;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Blank fixture for testing functionality of an empty database
    /// </summary>
    [TestFixture]
    public class ReadOnlyDatabaseFixture
    {
        private const string DatabaseFilename = "ReadOnlyFixture.ptd";
        private const string DatabaseTitle = "A database to test the read only boundary condition";
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

			// Load the database in read only mode
            this.freshDatabase = TimeSeriesDatabase.Read(
                DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadOnly);
        }

        [TearDown]
        protected void TearDown()
        {
            this.freshDatabase.Close();
            this.database.Delete();
        }

        [Test]
        [ExpectedException(typeof(TimeTagException))]
        public void TestReadOnlyStatePush()
        {
            this.freshDatabase.Push("Gauge", new Reading(14.0009, new DateTime(2005, 1, 1, 0, 1, 0)));
        }
    }
}
