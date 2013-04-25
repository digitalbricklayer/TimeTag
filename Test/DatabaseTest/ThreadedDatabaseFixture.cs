namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Threading;
    using NUnit.Framework;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Threaded fixture for testing locks in a multi-threaded scenario
    /// </summary>
    [TestFixture]
    public class ThreadedDatabaseFixture
    {
        private const string DatabaseFilename = "ThreadedFixture.ptd";
        private const string DatabaseTitle = "A database for testing locking functionality";
        private DateTime startTime = new DateTime(2005, 1, 1, 0, 0, 0);
        private TimeSpan Interval = new TimeSpan(0, 0, 1);
        private const int NumReadings = 100;
        private TimeSeriesDatabase database;
        private const int NumReadThreads = 10;
        private Thread writerThread;
        private List<Thread> readerThreads = new List<Thread>();

        [SetUp]
        protected void Setup()
        {
            TimeSeriesDatabaseTemplate template = new TimeSeriesDatabaseTemplate();
            template.Title = DatabaseTitle;
            template.StartTime = startTime;

            template.AddDataSource("Temperature", DataSource.ConversionFunctionType.Gauge, Interval, 10, 100);

            template.AddArchive("Average", ArchiveTemplate.ConsolidationFunctionType.Average, 50, 1, NumReadings);
            template.AddArchive("Max", ArchiveTemplate.ConsolidationFunctionType.Max, 50, 1, NumReadings);
            template.AddArchive("Min", ArchiveTemplate.ConsolidationFunctionType.Min, 50, 1, NumReadings);
            template.AddArchive("Last", ArchiveTemplate.ConsolidationFunctionType.Last, 50, 1, NumReadings);

            // Create the database on disk
            this.database = TimeSeriesDatabase.Create(DatabaseFilename, template);
            this.database.Close();
        }

        [TearDown]
        protected void TearDown()
        {
            this.database.Delete();
        }

        [Test]
        public void TestParrallelReadWriteSeperateInstances()
        {
            CreateThreadsSeperateInstances();
            StartThreads();
            WaitForThreadsToComplete();
            ValidateFinalDatabase();
        }

        [Test]
        public void TestParrallelReadWriteSameInstance()
        {
            // Database has been closed by the unit test setup, open it again
            this.database.Read(TimeSeriesDatabase.ConnectionMode.ReadWrite);

            CreateThreadsSameInstance();
            StartThreads();
            WaitForThreadsToComplete();
            ValidateFinalDatabase();
        }

        [Test]
        public void TestOrphanLockFile()
        {
            CreateUnlockedLockFile();
            /*
             * Read the database...it will wait indefinetely if the orphaned lock 
             * file stops the read, not great for unit testing but at least the
             * test will be broken.
             */
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(
                DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadOnly
                );
            ValidateIntermediateDatabase(db);
            db.Close();
        }

        [Test]
        [ExpectedException(typeof(TimeTagException))]
        public void TestReadOnlyFileAsWriteable()
        {
            FileAttributes savedDatabaseAttributes = File.GetAttributes(DatabaseFilename);
            File.SetAttributes(DatabaseFilename, FileAttributes.ReadOnly);
            try
            {
                // Should cause a TimeTagException
                TimeSeriesDatabase db = TimeSeriesDatabase.Read(
                    DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadWrite
                    );
            }
            finally
            {
                File.SetAttributes(DatabaseFilename, savedDatabaseAttributes);
            }
        }

        [Test]
        [ExpectedException(typeof(TimeTagException))]
        public void TestReadOnlyFileAsReadableOnly()
        {
            FileAttributes savedDatabaseAttributes = File.GetAttributes(DatabaseFilename);
            File.SetAttributes(DatabaseFilename, FileAttributes.ReadOnly);
            TimeSeriesDatabase db = TimeSeriesDatabase.Read(
                DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadOnly
                );
            ValidateIntermediateDatabase(db);
            try
            {
                // Should cause a TimeTagException because you can't modify a read-only database
                DateTime newTimestamp = this.startTime + Interval;
                db.Push("Temperature", new Reading(10D, newTimestamp));
            }
            finally
            {
                db.Close();
                File.SetAttributes(DatabaseFilename, savedDatabaseAttributes);
            }
        }

        private void CreateThreadsSameInstance()
        {
            // Create a single writer thread
            this.writerThread = new Thread(new ThreadStart(this.WriterThreadSameInstance));

            // Create the reader threads
            for (int i = 0; i < NumReadThreads; i++)
            {
                this.readerThreads.Add(new Thread(new ThreadStart(this.ReaderThreadSameInstance)));
            }
        }

        private void CreateUnlockedLockFile()
        {
            /*
             * Create a dummy lock file that simulates the case of a lock file 
             * being orphaned for some reason ie a lock happening and not being 
             * subsequently unlocked
             */
            FileInfo dummyLockFile = new FileInfo(DatabaseFilename + ".lock");
            StreamWriter w = dummyLockFile.CreateText();
            w.Close();
        }

        private void CreateThreadsSeperateInstances()
        {
            // Create a single writer thread
            this.writerThread = new Thread(new ThreadStart(this.WriterThreadSeperateInstances));

            // Create the reader threads
            for (int i = 0; i < NumReadThreads; i++)
            {
                this.readerThreads.Add(new Thread(new ThreadStart(this.ReaderThreadSeperateInstances)));
            }
        }

        private void StartThreads()
        {
            this.writerThread.Start();
            foreach (Thread t in this.readerThreads)
            {
                t.Start();
            }
        }

        private void ReaderThreadSeperateInstances()
        {
            for (int i = 0; i < NumReadings; i++)
            {
                TimeSeriesDatabase db = TimeSeriesDatabase.Read(DatabaseFilename);
                ValidateIntermediateDatabase(db);
                db.Close();
                Thread.Sleep(10);
            }
        }

        private void WriterThreadSeperateInstances()
        {
            DateTime newTimestamp = this.startTime + Interval;
            for (int i = 0; i < NumReadings; i++)
            {
                TimeSeriesDatabase db = TimeSeriesDatabase.Read(
                    DatabaseFilename, TimeSeriesDatabase.ConnectionMode.ReadWrite
                    );
                db.Push("Temperature", new Reading(10D, newTimestamp));
                db.Close();
                newTimestamp += Interval;
                Thread.Sleep(10);
            }
        }

        private void ReaderThreadSameInstance()
        {
            for (int i = 0; i < NumReadings; i++)
            {
                ValidateIntermediateDatabase(this.database);
                Thread.Sleep(10);
            }
        }

        private void WriterThreadSameInstance()
        {
            DateTime newTimestamp = this.startTime + Interval;
            for (int i = 0; i < NumReadings; i++)
            {
                this.database.Push("Temperature", new Reading(10D, newTimestamp));
                newTimestamp += Interval;
                Thread.Sleep(10);
            }
        }

        private void ValidateFinalDatabase()
        {
            this.database.Read();
            
            Assert.That(this.database.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(this.database.StartTime, Is.EqualTo(this.startTime));
            Assert.That(this.database.DataSources.Count, Is.EqualTo(1));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Average").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Average").DataPoints.Count, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Min").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Min").DataPoints.Count, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Max").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Max").DataPoints.Count, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Last").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(this.database.GetDataSourceByName("Temperature").GetArchiveByName("Last").DataPoints.Count, Is.EqualTo(NumReadings));

            this.database.Close();
        }

        private void ValidateIntermediateDatabase(TimeSeriesDatabase databaseToValidate)
        {
            Assert.That(databaseToValidate.Title, Is.EqualTo(DatabaseTitle));
            Assert.That(databaseToValidate.StartTime, Is.EqualTo(this.startTime));
            Assert.That(databaseToValidate.DataSources.Count, Is.EqualTo(1));
            Assert.That(databaseToValidate.GetDataSourceByName("Temperature").GetArchiveByName("Average").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(databaseToValidate.GetDataSourceByName("Temperature").GetArchiveByName("Min").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(databaseToValidate.GetDataSourceByName("Temperature").GetArchiveByName("Max").MaxDataPoints, Is.EqualTo(NumReadings));
            Assert.That(databaseToValidate.GetDataSourceByName("Temperature").GetArchiveByName("Last").MaxDataPoints, Is.EqualTo(NumReadings));
        }

        private void WaitForThreadsToComplete()
        {
            this.writerThread.Join();
            this.writerThread = null;
            foreach (Thread thread in this.readerThreads)
            {
                thread.Join();
            }
            this.readerThreads.Clear();
        }
    }
}
