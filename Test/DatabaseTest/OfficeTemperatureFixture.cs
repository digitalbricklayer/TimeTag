namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using NUnit.Framework;
    using NUnit.Framework.SyntaxHelpers;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Test fixture using data taken from an AKCP securityProbe environment 
    /// monitor in the OPENXTRA office server cabinet
    /// </summary>
    [TestFixture]
    public class OfficeTemperatureFixture
    {
        private const string DatabaseFilename = "OpenxtraServerCab.ptd";
        private const string DatabaseTitle = "Environmental conditions in the OPENXTRA server cabinet";
        private readonly TimeSpan PollingInterval = new TimeSpan(0, 15, 0);

        private const string TemperateDataSourceName = "Temperature";
        private const string HumidityDataSourceName = "Relative humidity";
        private const string LightDataSourceName = "Light";
        private const DataSource.ConversionFunctionType DataSourceType = DataSource.ConversionFunctionType.Gauge;

        private const string AllArchiveName = "All";
        private const ArchiveTemplate.ConsolidationFunctionType AllArchiveType = ArchiveTemplate.ConsolidationFunctionType.Average;
        private const int AllNumDataPointsPerArchive = 96;
        private const int AllReadingsPerDataPoint = 1;

        private const string MinArchiveName = "Low";
        private const ArchiveTemplate.ConsolidationFunctionType MinArchiveType = ArchiveTemplate.ConsolidationFunctionType.Min;
        private const int MinNumDataPointsPerArchive = 96;
        private const int MinActualNumDataPointsPerArchive = 8;
        private const int MinReadingsPerDataPoint = 12;

        private const string MaxArchiveName = "High";
        private const ArchiveTemplate.ConsolidationFunctionType MaxArchiveType = ArchiveTemplate.ConsolidationFunctionType.Max;
        private const int MaxNumDataPointsPerArchive = 96;
        private const int MaxActualNumDataPointsPerArchive = 8;
        private const int MaxReadingsPerDataPoint = 12;

        private const string AvgArchiveName = "1 hr Average";
        private const ArchiveTemplate.ConsolidationFunctionType AvgArchiveType = ArchiveTemplate.ConsolidationFunctionType.Average;
        private const int AvgNumDataPointsPerArchive = 24;
        private const int AvgReadingsPerDataPoint = 4;

        private const string LastArchiveName = "Last";
        private const ArchiveTemplate.ConsolidationFunctionType LastArchiveType = ArchiveTemplate.ConsolidationFunctionType.Last;
        private const int LastNumDataPointsPerArchive = 4;
        private const int LastReadingsPerDataPoint = 24;

        private const double MaxThreshold = 100D;
        private const double MinThreshold = 15D;

        private const double LightMaxThreshold = 100D;
        private const double LightMinThreshold = 0D;

        private const int XFactor = 50;
        private static readonly DateTime StartTime = new DateTime(2008, 3, 19, 23, 45, 0);
        private TimeSeriesDatabase database;
        private TimeSeriesDatabase freshDatabase;

        private int numTemperatureReadingsPushed;

        private int numHumidityReadingsPushed;

        [SetUp]
        protected void Setup()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            template.StartTime = StartTime;

            template.AddDataSource(HumidityDataSourceName, DataSourceType, PollingInterval, MinThreshold, MaxThreshold);
            template.AddDataSource(LightDataSourceName, DataSourceType, PollingInterval, LightMinThreshold, LightMaxThreshold);
            template.AddDataSource(TemperateDataSourceName, DataSourceType, PollingInterval, MinThreshold, MaxThreshold);

            template.AddArchive(
                AllArchiveName, AllArchiveType, XFactor, AllReadingsPerDataPoint, AllNumDataPointsPerArchive
                );
            template.AddArchive(
                AvgArchiveName, AvgArchiveType, XFactor, AvgReadingsPerDataPoint, AvgNumDataPointsPerArchive
                );
            template.AddArchive(
                MinArchiveName, MinArchiveType, XFactor, MinReadingsPerDataPoint, MinNumDataPointsPerArchive
                );
            template.AddArchive(
                MaxArchiveName, MaxArchiveType, XFactor, MaxReadingsPerDataPoint, MaxNumDataPointsPerArchive
                );
            template.AddArchive(
                LastArchiveName, LastArchiveType, XFactor, LastReadingsPerDataPoint, LastNumDataPointsPerArchive
                );

            // Create the database to disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);

            Reading[] readings = 
            {
                new Reading(25.0, new DateTime(2008, 3, 20, 0, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 0, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 0, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 0, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 1, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 1, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 1, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 1, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 2, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 2, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 2, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 2, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 3, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 3, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 3, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 4, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 4, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 4, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 4, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 5, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 5, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 5, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 5, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 6, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 6, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 6, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 6, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 7, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 7, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 7, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 7, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 8, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 8, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 8, 30, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 8, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 9, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 9, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 9, 30, 0)),
                new Reading(26.0, new DateTime(2008, 3, 20, 9, 45, 0)),
                new Reading(26.0, new DateTime(2008, 3, 20, 10, 0, 0)),
                new Reading(26.0, new DateTime(2008, 3, 20, 10, 15, 0)),
                new Reading(26.0, new DateTime(2008, 3, 20, 10, 30, 0)),
                new Reading(26.0, new DateTime(2008, 3, 20, 10, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 11, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 11, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 11, 30, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 11, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 20, 12, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 12, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 12, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 12, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 13, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 13, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 13, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 13, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 14, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 14, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 14, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 14, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 15, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 15, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 15, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 16, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 16, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 16, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 16, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 17, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 17, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 17, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 17, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 18, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 18, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 18, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 18, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 19, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 20, 19, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 19, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 19, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 20, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 20, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 20, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 20, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 21, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 21, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 21, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 21, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 22, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 22, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 22, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 22, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 23, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 23, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 23, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 20, 23, 45, 0))
            };

            this.numTemperatureReadingsPushed = readings.Length;

            this.database.Push(TemperateDataSourceName, readings);

            /*
             * Only push a single reading into the humidity data source. Then 
             * the fixture will be testing the case of a full archive with 
             * wrap around, a partially full archive, and an empty archive
             */
            this.database.Push(
                HumidityDataSourceName, new Reading(45.2D, new DateTime(2008, 3, 20, 0, 0, 0))
                );

            /*
             * Push a reading that will be discarded because it has the same 
             * timestamp as the last reading pushing into the database
             */
            this.database.Push(
                TemperateDataSourceName, new Reading(23.0, new DateTime(2008, 3, 20, 23, 45, 0)));

            this.numHumidityReadingsPushed = 1;

            // Close the connection to the database, otherwise we can't read the database back in
            this.database.Close();

            /*
             * Load the database so that we can check whether the data has been 
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
        public void TestValidateData()
        {
            ValidateOriginalDatabase(this.database);
            ValidateOriginalDatabase(this.freshDatabase);
        }

        [Test]
        public void TestValidateDataWithFilter1()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(new DateTime(2008, 3, 20, 0, 45, 1));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 19, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 0, 0))
            };

            Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
        }

        [Test]
        public void TestValidateDataWithFilter2()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            // From is specified after the last data point timestamp
            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(new DateTime(2008, 3, 20, 21, 0, 1));
            Assert.That(dataPoints.Length, Is.EqualTo(0));
        }

        [Test]
        public void TestValidateDataWithFilter3()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(
                new DateTime(2008, 3, 20, 0, 45, 1), new DateTime(2008, 3, 20, 19, 30, 1));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 19, 30, 0)),
            };

            Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
        }

        [Test]
        public void TestValidateDataWithFilter4()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);
            // From and to are specified inside the valid date range but doesn't intersect a data point timestamp
            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(
                new DateTime(2008, 3, 20, 3, 0, 1), new DateTime(2008, 3, 20, 3, 0, 2));
            Assert.That(dataPoints.Length, Is.EqualTo(0));
        }

        [Test]
        public void TestValidateDataWithFilter5()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            // From (and to) is specified after the last data point timestamp
            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(
                new DateTime(2008, 3, 20, 23, 45, 1), new DateTime(2008, 3, 20, 23, 59, 59));
            Assert.That(dataPoints.Length, Is.EqualTo(0));
        }

        [Test]
        public void TestValidateDataWithFilter6()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            // From (and to) is specified before the first data point timestamp
            DataPoint[] dataPoints = archive.DataPoints.FilterByTime(
                new DateTime(2008, 3, 19, 23, 59, 58), new DateTime(2008, 3, 19, 23, 59, 59));
            Assert.That(dataPoints.Length, Is.EqualTo(0));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestDataPointFilterToBeforeFrom()
        {
            Archive archive = this.database.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(MinArchiveName);

            // To is before the from argument
            archive.DataPoints.FilterByTime(new DateTime(2008, 3, 20, 23, 45, 2), new DateTime(2008, 3, 20, 23, 45, 1));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestDuplicateDataSourceName()
        {
            // Try to create a data source that has already been used
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSourceType, PollingInterval, MinThreshold, MaxThreshold);
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBlankDataSourceName()
        {
            string blankName = "";
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(blankName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestDataSourceNameTooBig()
        {
            string nameTooBig = new string('A', DataSource.MaxNameLength + 1);
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(nameTooBig, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
        }

        [Test]
        public void TestDataSourceNameMax()
        {
            // DS name is maxThreshold size
            string maxName = new string('A', DataSource.MaxNameLength);
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(maxName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            Assert.That(maxName, Is.EqualTo(template.GetDataSourceByName(maxName).Name));
        }

        [Test]
        public void TestGetDataSourceNameCaseInsensitive()
        {
            string allCaps = "AAA";
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(allCaps, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            Assert.That(template.GetDataSourceByName("aAa"), Is.Not.Null);
            Assert.That(allCaps, Is.EqualTo(template.GetDataSourceByName(allCaps).Name));
        }

        [Test]
        public void TestGetArchiveNameCaseInsensitive()
        {
            string allCapsArchiveName = "AAA";
            // Try to create an archive name that has already been used
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            template.AddArchive(allCapsArchiveName, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 100);
            Assert.That(template.GetArchiveByName("aAa"), Is.Not.Null);
            Assert.That(allCapsArchiveName, Is.EqualTo(template.GetArchiveByName(allCapsArchiveName).Name));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestDuplicateArchiveName()
        {
            string duplicateArchiveName = AvgArchiveName;
            // Try to create an archive name that has already been used
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            template.AddArchive(AvgArchiveName, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 100);
            template.AddArchive(duplicateArchiveName, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 100);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBlankArchiveName()
        {
            string blankName = String.Empty;
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            template.AddArchive(blankName, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 1000);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidArchiveName()
        {
            DataSource dataSource = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName);
            Assert.That(dataSource.Archives.GetArchiveByName("doesn't exist"), Is.Null);
        }

        [Test]
        public void TestInvalidArchiveTemplateName()
        {
            this.freshDatabase.GetDataSourceByName(TemperateDataSourceName);
            Assert.That(this.freshDatabase.ArchiveTemplates.GetArchiveTemplateByName("doesn't exist"), Is.Null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestArchiveNameTooBig()
        {
            string nameTooBig = new string('A', ArchiveTemplate.MaxNameLength + 1);
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            template.AddArchive(nameTooBig, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 1000);
        }

        [Test]
        public void TestArchiveNameMax()
        {
            string maxLengthName = new string('A', ArchiveTemplate.MaxNameLength);
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            template.AddArchive(maxLengthName, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, 1, 1000);
            ArchiveTemplate archiveTemplate = template.GetArchiveByName(maxLengthName);
            Assert.That(archiveTemplate.Name, Is.EqualTo(maxLengthName));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestThresholdOverlap()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            double minThreshold = 100D;
            double maxThreshold = 0D;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, minThreshold, maxThreshold);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestThresholdEqual()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, 100D, 100D);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestXFactorZero()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, 0D, 100D);
            template.AddArchive("X", ArchiveTemplate.ConsolidationFunctionType.Average, 0, 1, 100);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestXFactorNegative()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, 0D, 100D);
            template.AddArchive("X", ArchiveTemplate.ConsolidationFunctionType.Average, 0, -1, 100);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestXFactorTooHigh()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, 0D, 100D);
            template.AddArchive("X", ArchiveTemplate.ConsolidationFunctionType.Average, 0, 100, 100);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIntervalZero()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.StartTime = StartTime;
            template.AddDataSource(TemperateDataSourceName, DataSource.ConversionFunctionType.Gauge, new TimeSpan(0, 0, 0), 0D, 100D);
        }

        [Test]
        public void TestWrap()
        {
            Reading[] readings = 
            {
                new Reading(25.0, new DateTime(2008, 3, 21, 0, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 0, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 0, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 0, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 1, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 1, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 1, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 1, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 2, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 2, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 2, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 2, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 3, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 3, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 3, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 3, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 4, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 4, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 4, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 4, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 5, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 5, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 5, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 5, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 6, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 6, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 6, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 6, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 7, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 7, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 7, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 7, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 8, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 8, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 8, 30, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 8, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 9, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 9, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 9, 30, 0)),
                new Reading(26.0, new DateTime(2008, 3, 21, 9, 45, 0)),
                new Reading(26.0, new DateTime(2008, 3, 21, 10, 0, 0)),
                new Reading(26.0, new DateTime(2008, 3, 21, 10, 15, 0)),
                new Reading(26.0, new DateTime(2008, 3, 21, 10, 30, 0)),
                new Reading(26.0, new DateTime(2008, 3, 21, 10, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 11, 0, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 11, 15, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 11, 30, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 11, 45, 0)),
                new Reading(25.0, new DateTime(2008, 3, 21, 12, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 12, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 12, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 12, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 13, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 13, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 13, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 13, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 14, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 14, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 14, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 14, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 15, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 15, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 15, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 15, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 16, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 16, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 16, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 16, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 17, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 17, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 17, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 17, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 18, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 18, 15, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 18, 30, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 18, 45, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 19, 0, 0)),
                new Reading(24.0, new DateTime(2008, 3, 21, 19, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 19, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 19, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 20, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 20, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 20, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 20, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 30, 0)),
            };

            // Add sufficient data points to cause wrap around
            this.freshDatabase.Push(TemperateDataSourceName, readings);

            /*
             * Make sure the new data is valid using the database instance 
             * used to push the readings
             */
            ValidatePushDatabase(this.freshDatabase);
            this.freshDatabase.Close();

            /*
             * Read in the database into a fresh database instance and check to
             * make sure everything is valid
             */
            TimeSeriesDatabase newDatabase = TimeSeriesDatabase.Read(DatabaseFilename);
            ValidatePushDatabase(newDatabase);
            newDatabase.Close();

            /*
             * Read in the database into the original database (one that 
             * already has some internal state) instance and check to make 
             * sure everything is valid
             */
            this.database.Read();
            ValidatePushDatabase(this.database);
            this.database.Close();
        }

        [Test]
        [ExpectedException(typeof(TimeTagException))]
        public void TestDisconnectedStatePush()
        {
            this.database.Push(TemperateDataSourceName, new Reading(14.0009, new DateTime(2008, 3, 21, 0, 0, 0)));
        }

        [Test]
        public void TestMinInvalid()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(TemperateDataSourceName, new Reading(14.0009, new DateTime(2008, 3, 21, 0, 0, 0)));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestMaxInvalid()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(TemperateDataSourceName, new Reading(100.0001, new DateTime(2008, 3, 21, 0, 0, 0)));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestMinEqual()
        {
            Reading reading = new Reading(MinThreshold, new DateTime(2008, 3, 21, 0, 0, 0));

            // Test a reading equal to upper limit
            this.freshDatabase.Push(TemperateDataSourceName, reading);

            DataSource dataSource = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName);
            Archive archive = dataSource.GetArchiveByName(AllArchiveName);
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(archive.DataPoints.Count, Is.EqualTo(AllNumDataPointsPerArchive));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AllNumDataPointsPerArchive));

            DataPoint dataPoint = new DataPoint(reading.Value, reading.Timestamp);

            Assert.That(archive.DataPoints.GetAt(96), Is.EqualTo(dataPoint));
        }

        [Test]
        public void TestMaxEqual()
        {
            Reading maxReading = new Reading(MaxThreshold, new DateTime(2008, 3, 21, 0, 0, 0));

            // Push a reading equal to upper limit
            this.freshDatabase.Push(TemperateDataSourceName, maxReading);

            DataSource temperatureDataSource = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName);
            Archive allArchive = temperatureDataSource.GetArchiveByName(AllArchiveName);
            Assert.That(allArchive.XFactor, Is.EqualTo(XFactor));
            Assert.That(allArchive.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(allArchive.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(allArchive.DataPoints.Count, Is.EqualTo(AllNumDataPointsPerArchive));
            Assert.That(allArchive.MaxDataPoints, Is.EqualTo(AllNumDataPointsPerArchive));

            DataPoint dataPoint = new DataPoint(maxReading.Value, maxReading.Timestamp);

            Assert.That(allArchive.DataPoints.GetAt(96), Is.EqualTo(dataPoint));
        }

        [Test]
        public void TestPushPriorTimeNoRead()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(TemperateDataSourceName, new Reading(20.0, new DateTime(2008, 3, 20, 23, 44, 59)));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestPushSameTimeNoRead()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(TemperateDataSourceName, new Reading(20.0, new DateTime(2008, 3, 20, 23, 45, 0)));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestPushPriorTimeReRead()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(TemperateDataSourceName, new Reading(20.0, new DateTime(2008, 3, 20, 23, 44, 59)));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestPushSameTimeReRead()
        {
            long originalNumDiscarded = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded;
            long originalTotal = this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total;
            this.freshDatabase.Push(
                TemperateDataSourceName, new Reading(20.0, new DateTime(2008, 3, 20, 23, 45, 0))
                );
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Discarded, Is.EqualTo(originalNumDiscarded + 1));
            Assert.That(this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).Stats.Total, Is.EqualTo(originalTotal));
        }

        [Test]
        public void TestExport()
        {
            this.freshDatabase.Close();

            const string exportFile = "export.xml";

            // Export the original database to an XML file
            this.database.Export(exportFile);

            // Delete the original database
            this.database.Delete();

            // Import the XML contents into a newly created database
            TimeSeriesDatabase importDatabase = TimeSeriesDatabase.Import("ImportedDatabase.ptd", exportFile);
            ValidateOriginalDatabase(importDatabase);

            File.Delete(exportFile);
            importDatabase.Delete();
        }

        [Test]
        [ExpectedException(typeof(UnauthorizedAccessException))]
        public void TestExportReadOnly()
        {
            const string exportFile = "readonlyexportfile.xml";

            // Create a read-only file
            using (StreamWriter readOnlyExportFile = File.CreateText(exportFile))
            {
                readOnlyExportFile.Close();
            }
            File.SetAttributes(exportFile, FileAttributes.ReadOnly);

            try
            {
                // Try to export a file over a read-only file, should produce an exception
                this.database.Export(exportFile);
            }
            finally
            {
                File.SetAttributes(exportFile, FileAttributes.Normal);
                File.Delete(exportFile);
            }
        }

        [Test]
        public void TestImportMissingFile()
        {
            const string importFile = "ThisFileDoesntExist.xml";
            const string databaseFile = "ImportedDatabase.ptd";

            // The test is invalid if the import file actually exists
            Assert.That(File.Exists(importFile), Is.False);

            try
            {
                // Try to import a file that does not exist, should produce an exception
                TimeSeriesDatabase importDatabase = TimeSeriesDatabase.Import(databaseFile, importFile);
                Assert.Fail();
            }
            catch (FileNotFoundException)
            {
                // The database should not be created when an exception is created
                Assert.That(File.Exists(databaseFile), Is.False);
            }
        }

        [Test]
        [ExpectedException(typeof(TimeTagException))]
        public void TestImportInvalidFile()
        {
            const string importFile = "InvalidImportFile.xml";
            const string databaseFile = "ImportedDatabase.ptd";

            // Create the badly formed XML import file
            StreamWriter f = File.CreateText(importFile);
            f.WriteLine(@"<?xml version=""1.0"" encoding=""utf-8""?><TimeSeriesDatabase><Title>Environmental conditions in the OPENXTRA server cabinet</Title><StartTime>2008-03-19T23:45:00</StartTime");
            f.Close();

            try
            {
                // Try to import a badly formed file, should produce an exception
                TimeSeriesDatabase importDatabase = TimeSeriesDatabase.Import(databaseFile, importFile);
                Assert.Fail();
            }
            catch (TimeoutException)
            {
                // The database should not be created when an exception is created
                Assert.That(File.Exists(databaseFile), Is.False);
            }
            finally
            {
                File.Delete(importFile);
            }
        }

        [Test]
        public void TestSetTitle()
        {
            string newTitle = "Some other title";
            this.freshDatabase.SetTitle(newTitle);
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.Title, Is.EqualTo(newTitle));
            db.Close();
        }

        [Test]
        public void TestSetLongerTitle()
        {
            // Max size title to maximise chances of inducing an overwrite bug
            string newTitle = new string('A', TimeSeriesDatabase.MaxTitleLength);
            this.freshDatabase.SetTitle(newTitle);
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.Title, Is.EqualTo(newTitle));
            ValidateOriginalDatabase(db, false);
            db.Close();
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetTitleTooLong()
        {
            string nameTooBig = new string('A', TimeSeriesDatabase.MaxTitleLength + 1);
            try
            {
                this.freshDatabase.SetTitle(nameTooBig);
            }
            finally
            {
                this.freshDatabase.Close();
            }
        }

        [Test]
        public void TestSetDataSourceName()
        {
            string newDataSourceName = "Temp";
            this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).SetName(newDataSourceName);
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.GetDataSourceByName(newDataSourceName).Name, Is.EqualTo(newDataSourceName));
            db.Close();
        }

        [Test]
        public void TestSetDataSourceNameLonger()
        {
            // Max size name to maximise chances of inducing an overwrite bug
            string newDataSourceName = new string('A', DataSource.MaxNameLength);
            this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).SetName(newDataSourceName);
            Assert.That(this.freshDatabase.GetDataSourceByName(newDataSourceName).Name, Is.EqualTo(newDataSourceName));
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.GetDataSourceByName(newDataSourceName).Name, Is.EqualTo(newDataSourceName));
            ValidateOriginalTemperatureDataSource(db.GetDataSourceByName(newDataSourceName), false);
            db.Close();
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetDataSourceNameTooLong()
        {
            string nameTooBig = new string('A', DataSource.MaxNameLength + 1);
            try
            {
                this.freshDatabase.GetDataSourceByName(TemperateDataSourceName).SetName(nameTooBig);
            }
            finally
            {
                this.freshDatabase.Close();
            }
        }

        [Test]
        public void TestSetArchiveTemplateName()
        {
            string newArchiveName = "A";
            this.freshDatabase.GetArchiveTemplateByName(AllArchiveName).SetName(newArchiveName);
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.GetArchiveTemplateByName(newArchiveName).Name, Is.EqualTo(newArchiveName));
            db.Close();
        }

        [Test]
        public void TestSetArchiveTemplateNameLonger()
        {
            // Max size name to maximise chances of inducing an overwrite bug
            string newArchiveName = new string('A', ArchiveTemplate.MaxNameLength);
            this.freshDatabase.GetArchiveTemplateByName(AllArchiveName).SetName(newArchiveName);
            Assert.That(this.freshDatabase.GetArchiveTemplateByName(newArchiveName).Name, Is.EqualTo(newArchiveName));
            this.freshDatabase.Close();
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
            Assert.That(db.GetArchiveTemplateByName(newArchiveName).Name, Is.EqualTo(newArchiveName));
            ValidateOriginalTemperatureAllArchive(db.GetDataSourceByName(TemperateDataSourceName).GetArchiveByName(newArchiveName), false);
            db.Close();
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetArchiveTemplateNameTooLong()
        {
            string nameTooBig = new string('A', ArchiveTemplate.MaxNameLength + 1);
            try
            {
                this.freshDatabase.GetArchiveTemplateByName(AllArchiveName).SetName(nameTooBig);
            }
            finally
            {
                this.freshDatabase.Close();
            }
        }

        private void ValidateOriginalDatabase(TimeSeriesDatabase databaseToValidate)
        {
            ValidateOriginalDatabase(databaseToValidate, true);
        }

        private void ValidateOriginalDatabase(TimeSeriesDatabase databaseToValidate, bool validateText)
        {
            if (validateText)
            {
                Assert.That(databaseToValidate.Title, Is.EqualTo(DatabaseTitle));
            }
            Assert.That(databaseToValidate.StartTime, Is.EqualTo(StartTime));
            Assert.That(databaseToValidate.DataSources.Count, Is.EqualTo(3));
            Assert.That(databaseToValidate.Stats.Total, Is.EqualTo(this.numTemperatureReadingsPushed + this.numHumidityReadingsPushed));
            Assert.That(databaseToValidate.Stats.Discarded, Is.EqualTo(1));
            Assert.That(databaseToValidate.ArchiveTemplates.Count, Is.EqualTo(5));

            ValidateOriginalTemperatureDataSource(databaseToValidate.GetDataSourceByName(TemperateDataSourceName), validateText);
            ValidateOriginalHumidityDataSource(databaseToValidate.GetDataSourceByName(HumidityDataSourceName), validateText);
            ValidateOriginalLightDataSource(databaseToValidate.GetDataSourceByName(LightDataSourceName), validateText);
        }

        private void ValidateOriginalTemperatureDataSource(DataSource temperatureDataSource, bool validateText)
        {
            if (validateText)
            {
                Assert.That(temperatureDataSource.Name, Is.EqualTo(TemperateDataSourceName));
            }
            Assert.That(temperatureDataSource.ConversionFunction, Is.EqualTo(DataSourceType));
            Assert.That(temperatureDataSource.PollingInterval, Is.EqualTo(PollingInterval));
            Assert.That(temperatureDataSource.Range.Max, Is.EqualTo(MaxThreshold));
            Assert.That(temperatureDataSource.Range.Min, Is.EqualTo(MinThreshold));
            Assert.That(temperatureDataSource.LastUpdateTimestamp, Is.EqualTo(new DateTime(2008, 3, 20, 23, 45, 0)));
            Assert.That(temperatureDataSource.Archives.Count, Is.EqualTo(5));
            Assert.That(temperatureDataSource.Stats.Total, Is.EqualTo(this.numTemperatureReadingsPushed));
            Assert.That(temperatureDataSource.Stats.Discarded, Is.EqualTo(1));
            Assert.That(temperatureDataSource.LastReading, Is.EqualTo(new Reading(23.0, new DateTime(2008, 3, 20, 23, 45, 0))));
            ValidateOriginalTemperatureAllArchive(temperatureDataSource.GetArchiveByName(AllArchiveName), validateText);
            ValidateOriginalTemperatureAvgArchive(temperatureDataSource.GetArchiveByName(AvgArchiveName), validateText);
            ValidateOriginalTemperatureLastArchive(temperatureDataSource.GetArchiveByName(LastArchiveName), validateText);
            ValidateOriginalTemperatureMinArchive(temperatureDataSource.GetArchiveByName(MinArchiveName), validateText);
            ValidateOriginalTemperatureMaxArchive(temperatureDataSource.GetArchiveByName(MaxArchiveName), validateText);
        }

        private void ValidateOriginalHumidityDataSource(DataSource humidityDataSource, bool validateText)
        {
            if (validateText)
            {
                Assert.That(humidityDataSource.Name, Is.EqualTo(HumidityDataSourceName));
            }
            Assert.That(humidityDataSource.ConversionFunction, Is.EqualTo(DataSourceType));
            Assert.That(humidityDataSource.PollingInterval, Is.EqualTo(PollingInterval));
            Assert.That(humidityDataSource.Range.Max, Is.EqualTo(MaxThreshold));
            Assert.That(humidityDataSource.Range.Min, Is.EqualTo(MinThreshold));
            Assert.That(humidityDataSource.Archives.Count, Is.EqualTo(5));
            Assert.That(humidityDataSource.Stats.Total, Is.EqualTo(1));
            Assert.That(humidityDataSource.Stats.Discarded, Is.EqualTo(0));
            Assert.That(humidityDataSource.LastReading, Is.EqualTo(new Reading(45.2D, new DateTime(2008, 3, 20, 0, 0, 0))));
            ValidateOriginalHumidityAllArchive(humidityDataSource.GetArchiveByName(AllArchiveName));
            ValidateOriginalHumidityAvgArchive(humidityDataSource.GetArchiveByName(AvgArchiveName));
            ValidateOriginalHumidityMinArchive(humidityDataSource.GetArchiveByName(MinArchiveName));
            ValidateOriginalHumidityMaxArchive(humidityDataSource.GetArchiveByName(MaxArchiveName));
        }

        private void ValidateOriginalLightDataSource(DataSource lightDataSource, bool validateText)
        {
            if (validateText)
            {
                Assert.That(lightDataSource.Name, Is.EqualTo(LightDataSourceName));
            }
            Assert.That(lightDataSource.ConversionFunction, Is.EqualTo(DataSourceType));
            Assert.That(lightDataSource.PollingInterval, Is.EqualTo(PollingInterval));
            Assert.That(lightDataSource.Range.Max, Is.EqualTo(LightMaxThreshold));
            Assert.That(lightDataSource.Range.Min, Is.EqualTo(LightMinThreshold));
            Assert.That(lightDataSource.LastUpdateTimestamp, Is.EqualTo(DateTime.MinValue));
            Assert.That(lightDataSource.Archives.Count, Is.EqualTo(5));
            Assert.That(lightDataSource.Stats.Total, Is.EqualTo(0));
            Assert.That(lightDataSource.Stats.Discarded, Is.EqualTo(0));
            Assert.That(lightDataSource.LastReading, Is.Null);
        }

        private void ValidateOriginalTemperatureMinArchive(Archive archive, bool validateText)
        {
            if (validateText)
            {
                Assert.That(archive.Name, Is.EqualTo(MinArchiveName));
            }
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MinArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MinReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MinNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.0, new DateTime(2008, 3, 20, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 19, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 0, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(MinActualNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private static void ValidateOriginalTemperatureMaxArchive(Archive archive, bool validateText)
        {
            if (validateText)
            {
                Assert.That(archive.Name, Is.EqualTo(MaxArchiveName));
                Assert.That(archive.Template.Name, Is.EqualTo(MaxArchiveName));
            }
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MaxArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MaxReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MaxNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(25.0, new DateTime(2008, 3, 20, 0, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 8, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 9, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 12, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 0, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(MaxActualNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateOriginalTemperatureAllArchive(Archive archive, bool validateText)
        {
            if (validateText)
            {
                Assert.That(archive.Name, Is.EqualTo(AllArchiveName));
                Assert.That(archive.Template.Name, Is.EqualTo(AllArchiveName));
            }
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AllNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(25.0, new DateTime(2008, 3, 20, 0, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 0, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 0, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 1, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 1, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 1, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 1, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 2, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 2, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 2, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 2, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 4, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 4, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 4, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 4, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 7, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 7, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 7, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 7, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 8, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 8, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 8, 30, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 8, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 9, 30, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 9, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 10, 0, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 10, 15, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 10, 30, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 10, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 30, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 12, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 12, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 13, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 13, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 13, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 13, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 14, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 14, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 14, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 14, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 16, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 16, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 16, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 16, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 19, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 19, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 19, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 19, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 20, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 20, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 20, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 20, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 22, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 22, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 22, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 22, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 45, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(AllNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateOriginalTemperatureAvgArchive(Archive archive, bool validateText)
        {
            if (validateText)
            {
                Assert.That(archive.Name, Is.EqualTo(AvgArchiveName));
                Assert.That(archive.Template.Name, Is.EqualTo(AvgArchiveName));
            }
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AvgArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AvgReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AvgNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.75, new DateTime(2008, 3, 20, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 1, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 2, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 3, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 4, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 6, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 7, 45, 0)),
                new DataPoint(24.25, new DateTime(2008, 3, 20, 8, 45, 0)),
                new DataPoint(25.25, new DateTime(2008, 3, 20, 9, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 20, 10, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 45, 0)),
                new DataPoint(24.25, new DateTime(2008, 3, 20, 12, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 13, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 14, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 15, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 16, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 18, 45, 0)),
                new DataPoint(23.5, new DateTime(2008, 3, 20, 19, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 20, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 21, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 22, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 45, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(AvgNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateOriginalTemperatureLastArchive(Archive archive, bool validateText)
        {
            if (validateText)
            {
                Assert.That(archive.Name, Is.EqualTo(LastArchiveName));
                Assert.That(archive.Template.Name, Is.EqualTo(LastArchiveName));
            }
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(LastArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(LastReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(LastNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.0, new DateTime(2008, 3, 20, 5, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 20, 11, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 20, 17, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 20, 23, 45, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(LastNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateOriginalHumidityMinArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MinArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MinReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MinNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Reading[] expectedReadings = 
            {
                new Reading(45.2, new DateTime(2008, 3, 20, 0, 0, 0))
            };
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(1));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));

            // Validate data point collection
            Assert.That(archive.DataPoints.Count, Is.EqualTo(0));
        }

        private void ValidateOriginalHumidityMaxArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MaxArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MaxReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MaxNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Reading[] expectedReadings = 
            {
                new Reading(45.2, new DateTime(2008, 3, 20, 0, 0, 0))
            };
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(expectedReadings.Length));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));
            Assert.That(archive.AccumulatedReadings.GetAt(1), Is.EqualTo(expectedReadings[0]));

            // Validate data point collection
            Assert.That(archive.DataPoints.Count, Is.EqualTo(0));
        }

        private void ValidateOriginalHumidityAllArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AllNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(45.2, new DateTime(2008, 3, 20, 0, 0, 0))
            };
            Assert.That(archive.DataPoints.Count, Is.EqualTo(1));
            Assert.That(archive.DataPoints.ToArray(), Is.EqualTo(expectedDataPoints));
        }

        private void ValidateOriginalHumidityAvgArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AvgArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AvgReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AvgNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Reading[] expectedReadings = 
            {
                new Reading(45.2, new DateTime(2008, 3, 20, 0, 0, 0))
            };
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(1));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));

            // Validate data point collection
            Assert.That(archive.DataPoints.Count, Is.EqualTo(0));
        }

        private void ValidatePushDatabase(TimeSeriesDatabase databaseToValidate)
        {
            Assert.That(databaseToValidate.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(databaseToValidate.StartTime, Is.EqualTo(StartTime));
            Assert.That(databaseToValidate.DataSources.Count, Is.EqualTo(3));

            DataSource temperatureDataSource = databaseToValidate.GetDataSourceByName(TemperateDataSourceName);
            Assert.That(temperatureDataSource.Name, Is.EqualTo(TemperateDataSourceName));
            Assert.That(temperatureDataSource.ConversionFunction, Is.EqualTo(DataSourceType));
            Assert.That(temperatureDataSource.PollingInterval, Is.EqualTo(PollingInterval));
            Assert.That(temperatureDataSource.Range.Max, Is.EqualTo(MaxThreshold));
            Assert.That(temperatureDataSource.Range.Min, Is.EqualTo(MinThreshold));
            Assert.That(temperatureDataSource.Archives.Count, Is.EqualTo(5));
            ValidatePushTemperatureArchives(temperatureDataSource);

            DataSource humidityDataSource = databaseToValidate.GetDataSourceByName(HumidityDataSourceName);
            Assert.That(humidityDataSource.Name, Is.EqualTo(HumidityDataSourceName));
            Assert.That(humidityDataSource.ConversionFunction, Is.EqualTo(DataSourceType));
            Assert.That(humidityDataSource.PollingInterval, Is.EqualTo(PollingInterval));
            Assert.That(humidityDataSource.Range.Max, Is.EqualTo(MaxThreshold));
            Assert.That(humidityDataSource.Range.Min, Is.EqualTo(MinThreshold));
            Assert.That(humidityDataSource.Archives.Count, Is.EqualTo(5));
            ValidatePushHumidityArchives(humidityDataSource);
        }

        private void ValidatePushTemperatureArchives(DataSource dataSource)
        {
            ValidatePushTemperatureAllArchive(dataSource.GetArchiveByName(AllArchiveName));
            ValidatePushTemperatureMinArchive(dataSource.GetArchiveByName(MinArchiveName));
            ValidatePushTemperatureMaxArchive(dataSource.GetArchiveByName(MaxArchiveName));
            ValidatePushTemperatureAvgArchive(dataSource.GetArchiveByName(AvgArchiveName));
        }

        private void ValidatePushHumidityArchives(DataSource dataSource)
        {
            // Nothing is pushed into the humidity data point after the first reading
            ValidateOriginalHumidityAllArchive(dataSource.GetArchiveByName(AllArchiveName));
            ValidateOriginalHumidityMinArchive(dataSource.GetArchiveByName(MinArchiveName));
            ValidateOriginalHumidityMaxArchive(dataSource.GetArchiveByName(MaxArchiveName));
            ValidateOriginalHumidityAvgArchive(dataSource.GetArchiveByName(AvgArchiveName));
        }

        private void ValidatePushTemperatureMinArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MinArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MinReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MinNumDataPointsPerArchive));

            Reading[] expectedReadings =
            {
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 30, 0)),
            };

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(expectedReadings.Length));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.0, new DateTime(2008, 3, 21, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 9, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 19, 30, 0)),
            };

            Assert.That(archive.DataPoints.Count, Is.EqualTo(15));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 45, 0)), Is.EqualTo(expectedDataPoints));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 45, 0), new DateTime(2008, 3, 21, 21, 0, 0)), Is.EqualTo(expectedDataPoints));
        }

        private void ValidatePushTemperatureMaxArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(MaxArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(MaxReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(MaxNumDataPointsPerArchive));

            Reading[] expectedReadings =
            {
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 21, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 30, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 22, 45, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 30, 0)),
            };

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(expectedReadings.Length));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(25.0, new DateTime(2008, 3, 21, 0, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 8, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 9, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 12, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 0, 0)),
            };

            Assert.That(archive.DataPoints.Count, Is.EqualTo(15));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 0, 0)), Is.EqualTo(expectedDataPoints));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 0, 0), new DateTime(2008, 3, 21, 21, 0, 0)), Is.EqualTo(expectedDataPoints));
        }

        private void ValidatePushTemperatureAllArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AllArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AllReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AllNumDataPointsPerArchive));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(25.0, new DateTime(2008, 3, 21, 0, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 0, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 0, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 1, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 1, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 1, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 1, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 2, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 2, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 2, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 2, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 4, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 4, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 4, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 4, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 5, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 5, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 5, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 5, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 7, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 7, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 7, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 7, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 8, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 8, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 8, 30, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 8, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 9, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 9, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 9, 30, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 9, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 10, 0, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 10, 15, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 10, 30, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 10, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 11, 0, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 11, 15, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 11, 30, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 11, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 12, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 12, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 12, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 12, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 13, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 13, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 13, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 13, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 14, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 14, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 14, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 14, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 16, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 16, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 16, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 16, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 17, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 17, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 17, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 17, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 15, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 30, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 19, 0, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 19, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 19, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 19, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 20, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 20, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 20, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 20, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 21, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 21, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 21, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 21, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 22, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 22, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 22, 30, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 22, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 23, 0, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 23, 15, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 23, 30, 0)),
            };

            Assert.That(archive.DataPoints.Count, Is.EqualTo(AllNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 0, 0)), Is.EqualTo(expectedDataPoints));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 0, 0), new DateTime(2008, 3, 21, 23, 45, 0)), Is.EqualTo(expectedDataPoints));
        }

        private void ValidatePushTemperatureAvgArchive(Archive archive)
        {
            Assert.That(archive.XFactor, Is.EqualTo(XFactor));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(AvgArchiveType));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(AvgReadingsPerDataPoint));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(AvgNumDataPointsPerArchive));

            Reading[] expectedReadings =
            {
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 0, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 15, 0)),
                new Reading(23.0, new DateTime(2008, 3, 21, 23, 30, 0)),
            };

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(expectedReadings.Length));
            Assert.That(archive.AccumulatedReadings.ToArray(), Is.EqualTo(expectedReadings));

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(24.75, new DateTime(2008, 3, 21, 0, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 1, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 2, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 3, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 4, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 5, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 6, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 7, 45, 0)),
                new DataPoint(24.25, new DateTime(2008, 3, 21, 8, 45, 0)),
                new DataPoint(25.25, new DateTime(2008, 3, 21, 9, 45, 0)),
                new DataPoint(26.0, new DateTime(2008, 3, 21, 10, 45, 0)),
                new DataPoint(25.0, new DateTime(2008, 3, 21, 11, 45, 0)),
                new DataPoint(24.25, new DateTime(2008, 3, 21, 12, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 13, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 14, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 15, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 16, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 17, 45, 0)),
                new DataPoint(24.0, new DateTime(2008, 3, 21, 18, 45, 0)),
                new DataPoint(23.5, new DateTime(2008, 3, 21, 19, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 20, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 21, 45, 0)),
                new DataPoint(23.0, new DateTime(2008, 3, 21, 22, 45, 0)),
            };

            Assert.That(archive.DataPoints.Count, Is.EqualTo(AvgNumDataPointsPerArchive));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 45, 0)), Is.EqualTo(expectedDataPoints));
            Assert.That(archive.DataPoints.FilterByTime(new DateTime(2008, 3, 21, 0, 45, 0), new DateTime(2008, 3, 21, 23, 45, 0)), Is.EqualTo(expectedDataPoints));
        }
    }
}
