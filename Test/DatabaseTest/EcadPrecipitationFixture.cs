namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Diagnostics;
    using FileHelpers;
    using NUnit.Framework;
    using NUnit.Framework.SyntaxHelpers;
    using Openxtra.TimeTag.Database;             // The namespace under test

    /// <summary>
    /// ECAD precipation test fixture
    /// ECAD data set is available from the following:
    /// Klein Tank, A.M.G. and Coauthors, 2002. Daily dataset of 20th-century surface
    /// air temperature and precipitation series for the European Climate Assessment.
    /// Int. J. of Climatol., 22, 1441-1453.
    /// Data and metadata available at http://eca.knmi.nl
    /// /// </summary>
    [TestFixture, Explicit]
    [Category("Long Running")]
    public class EcadPrecipitationFixture
    {
        private const string DatabaseFilename = "EcadPrecipitationNoneBlendedDataset.ptd";
        private const string DatabaseTitle = "ECAD none blended precipitation data set";
        private const string ArchiveThreeSixFiveAvg = "365 day average";
        private const double MinThreshold = 0D;
        private const double MaxThreshold = 1000D;
        private readonly TimeSpan PollingInterval = new TimeSpan(1, 0, 0, 0);
        private const int XFactor = 50;
        private const int ReadingsPerDataPoint = 365;
        private const int MaxDataPoints = 200;

        // Relative path to the test data folder
        private const string TestDataFolderName = @"..\..\Test\Test Datasets\ECAD Precipitation";

        /*
         * Map the unique weather station source identifier (SOUID) to its name. The name isn't 
         * guaranteed to be unique
         */
        private Dictionary<long, string> souidToName = new Dictionary<long, string>();

        /*
         * Full path to the info file containing all the names and SOUID of 
         * all weather stations
         */
        private string infoFileFullPath;

        // ECAD weather station records
        private EcadInfoRecord[] infoRecords;

        // Timestamp of the first data point from all of the weather stations
        private DateTime startTime;

        // Database instance used for inserting the data
        private TimeSeriesDatabase database;

        // Database instance used to verify that the data has been properly persisted
        private TimeSeriesDatabase freshDatabase;

        [SetUp]
        protected void Setup()
        {
            this.infoFileFullPath = Path.GetFullPath(TestDataFolderName + @"\info.txt");

            Console.Out.WriteLine("Creating ECAD database");

            // Create the database template with all of the data sources required
            TimeSeriesDatabaseTemplate template = CreateDatabaseTemplate();

            // Create the empty database on disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);

            // Insert the readings from the text files into the database
            InsertReadingsIntoDatabase();

            // Creating the data leaves the database open, close it here so it can be read
            this.database.Close();

            Console.Out.WriteLine(
                string.Format("...done ({0}/{1})", this.database.Stats.Total, this.database.Stats.Discarded)
                );

            /*
             * Load the database so that we can check whether it has been 
             * persisted properly
             */
            this.freshDatabase = TimeSeriesDatabase.Read(DatabaseFilename);
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
            ValidateEcadDatabase(this.database);
            ValidateEcadDatabase(this.freshDatabase);
        }

        private void InsertReadingsIntoDatabase()
        {
            FileHelperEngine<EcadRecord> engine = new FileHelperEngine<EcadRecord>();

            /*
             * There are readings in the files that occur on invalid dates (like 
             * 29th Feb in none leap years and 31st April or 31st September for 
             * instance.) The readings with invalid dates are ignored.
             */
            engine.ErrorManager.ErrorMode = ErrorMode.IgnoreAndContinue;

            DirectoryInfo testDataFolder = new DirectoryInfo(Path.GetFullPath(TestDataFolderName));
            FileInfo[] files = testDataFolder.GetFiles("RR_SOUID*.txt", SearchOption.TopDirectoryOnly);
            foreach (FileInfo file in files)
            {
                // Parse the records out of the file
                EcadRecord[] records = engine.ReadFile(file.FullName);

                Debug.Assert(records.Length > 0);

                List<Reading> readings = new List<Reading>();

                // Convert the ECAD records into readings to be pushed into the database
                foreach (EcadRecord record in records)
                {
                    // Some of the readings aren't valid (Q_RR can have a value 
                    // 0-9, zero meaning absolutely certain 9 not at all 
                    // certain.) Only use the readings known to be absolutely certain
                    if (record.Q_RR == 0)
                    {
                        readings.Add(new Reading(record.RR, record.DATE));
                    }
                }

                Console.Out.WriteLine(
                    string.Format("Pushing {1} readings into data source {0}",
                    ConvertSouidToDataSourceName(records[0].SOUID), readings.Count)
                    );

                // Process all of the weather station readings en masse
                this.database.Push(ConvertSouidToDataSourceName(records[0].SOUID), readings.ToArray());
            }
        }

        private TimeSeriesDatabaseTemplate CreateDatabaseTemplate()
        {
            // Create an index of all weather stations...each weather station 
            // will get its own data source
            FileHelperEngine<EcadInfoRecord> engine = new FileHelperEngine<EcadInfoRecord>();
            this.infoRecords = engine.ReadFile(infoFileFullPath);

            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            DateTime earliestStartTime = FindEarliestStartTime();
            TimeSpan oneDay = new TimeSpan(1, 0, 0, 0);
            template.StartTime = this.startTime = earliestStartTime - oneDay;
            foreach (EcadInfoRecord record in infoRecords)
            {
                CreateDataSourceTemplate(template, record);
            }
            template.AddArchive(ArchiveThreeSixFiveAvg, ArchiveTemplate.ConsolidationFunctionType.Average, XFactor, ReadingsPerDataPoint, MaxDataPoints);

            return template;
        }

        private void CreateDataSourceTemplate(TimeSeriesDatabaseTemplate template, EcadInfoRecord record)
        {
            // SOUNAME isn't guaranteed to be unique
            string dataSourceName = record.COUNTRY + " " + record.SOUNAME + " " + record.SOUID;
            Console.Out.WriteLine(string.Format("Adding data source {0} to template", dataSourceName));
            template.AddDataSource(dataSourceName, DataSource.ConversionFunctionType.Gauge, PollingInterval, MinThreshold, MaxThreshold);
            souidToName[record.SOUID] = dataSourceName;
        }

        private string ConvertSouidToDataSourceName(long souID)
        {
            return this.souidToName[souID];
        }

        private void ValidateEcadDatabase(TimeSeriesDatabase databaseToTest)
        {
            Assert.That(databaseToTest.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(databaseToTest.StartTime, Is.EqualTo(this.startTime));
        }

        private DateTime FindEarliestStartTime()
        {
            DateTime earliestToDate = DateTime.MinValue;
            foreach (EcadInfoRecord record in infoRecords)
            {
                if (earliestToDate == DateTime.MinValue)
                {
                    // First record, it must be the earliest
                    earliestToDate = record.START;
                }
                else if (record.START < earliestToDate)
                {
                    earliestToDate = record.START;
                }
            }
            return earliestToDate;
        }
    }
}
