namespace Openxtra.TimeTag.Database.PowerShell.Test
{
    using System;
    using System.IO;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;
    using System.Management.Automation.Runspaces;
    using System.Management.Automation;
    using NUnit.Framework;
    using Database;

    /// <summary>
    /// Fixture for testing the TimeTag PowerShell snap-in
    /// </summary>
    [TestFixture]
    public class PowerShellFixture
    {
        private RunspaceConfiguration config;
        private Runspace runspace;

        [SetUp]
        protected void Setup()
        {
            this.config = RunspaceConfiguration.Create();
            PSSnapInException warning;
            this.config.AddPSSnapIn("TimeTagPowerShellSnapIn", out warning);
            this.runspace = RunspaceFactory.CreateRunspace(this.config);
            this.runspace.Open();
            CreateDatabase();
        }

        [TearDown]
        protected void TearDown()
        {
            DeleteDatabase();
            this.runspace.Close();
        }

        [Test]
        public void TestPush()
        {
            PushReadingsIntoDatabase();
            ValidateDatabase();
        }

        [Test]
        public void TestEmptyDatabaseStats()
        {
            DatabaseStats stats = GetDatabaseStats();
            Assert.That(stats.Total, Is.EqualTo(0));
            Assert.That(stats.Discarded, Is.EqualTo(0));
        }

        [Test]
        public void TestDatabaseImportExport()
        {
            PushReadingsIntoDatabase();
            ExportDatabase();
            DeleteDatabase();
            ImportDatabase();
            ValidateDatabase();
        }

        [Test]
        public void TestGetDataPointsRange()
        {
            PushChartReadingsIntoDatabase();

            {
                Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataPoints -Database PowerShellTest.ptd -DataSource Temperature -Archive All -Start ""10/07/2008 17:46:01""");
                Collection<PSObject> returnedObjects = cmd.Invoke();
                ValidatePipelineResponse(cmd);

                DataPoint[] dataPoints = ConvertReturnedObjectToDataPoints(returnedObjects);

                // Validate data point collection
                DataPoint[] expectedDataPoints = 
                {
                    new DataPoint(18.2D, new DateTime(2008, 7, 10, 17, 47, 0)),
                    new DataPoint(16.9D, new DateTime(2008, 7, 10, 17, 48, 0))
                };

                Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
                Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
            }

            {
                Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataPoints -Database PowerShellTest.ptd -DataSource Temperature -Archive All -Start ""10/07/2008 17:46:01"" -End ""10/07/2008 17:47:59""");
                Collection<PSObject> returnedObjects = cmd.Invoke();
                ValidatePipelineResponse(cmd);

                DataPoint[] dataPoints = ConvertReturnedObjectToDataPoints(returnedObjects);

                // Validate data point collection
                DataPoint[] expectedDataPoints = 
                {
                    new DataPoint(18.2D, new DateTime(2008, 7, 10, 17, 47, 0)),
                };

                Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
                Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
            }

            {
                Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataPoints -Database PowerShellTest.ptd -DataSource Temperature -Archive All -End ""10/07/2008 17:47:59""");
                Collection<PSObject> returnedObjects = cmd.Invoke();
                ValidatePipelineResponse(cmd);

                DataPoint[] dataPoints = ConvertReturnedObjectToDataPoints(returnedObjects);

                // Validate data point collection
                DataPoint[] expectedDataPoints = 
                {
                    new DataPoint(17D, new DateTime(2008, 7, 10, 17, 46, 0)),
                    new DataPoint(18.2D, new DateTime(2008, 7, 10, 17, 47, 0))
                };

                Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
                Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
            }
        }

        private void ExportDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Export-TTDatabase -Database PowerShellTest.ptd -ExportFile PowerShellTest.xml -Force");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void ImportDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Import-TTDatabase -Database PowerShellTest.ptd -ImportFile PowerShellTest.xml");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private DatabaseStats GetDatabaseStats()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTStats -Database PowerShellTest.ptd");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);
            return returnedObjects[0].ImmediateBaseObject as DatabaseStats;
        }

        private void CreateDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"New-TTDatabase -Database PowerShellTest.ptd -StartTime ""10/07/2008 17:45"" -Title ""PowerShell Unit Test Database"" -Force -DataSource Temperature:gauge:60:0:100,Absolute:absolute:60:0:100,Derive:derive:60:0:100,Counter:counter:60:0:100 -Archive All:average:50:1:96");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void DeleteDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Delete-TTDatabase -Database PowerShellTest.ptd");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void PushChartReadingsIntoDatabase()
        {
            Pipeline cmd1 = this.runspace.CreatePipeline(@"Add-TTReading -Database PowerShellTest.ptd -DataSource Temperature -Value 17.0 -TimeStamp ""10/07/2008 17:46""");
            cmd1.Invoke();
            ValidatePipelineResponse(cmd1);

            Pipeline cmd2 = this.runspace.CreatePipeline(@"Add-TTReading -Database PowerShellTest.ptd -DataSource Temperature -Value 18.2 -TimeStamp ""10/07/2008 17:47""");
            cmd2.Invoke();
            ValidatePipelineResponse(cmd2);

            Pipeline cmd3 = this.runspace.CreatePipeline(@"Add-TTReading -Database PowerShellTest.ptd -DataSource Temperature -Value 16.9 -TimeStamp ""10/07/2008 17:48""");
            cmd3.Invoke();
            ValidatePipelineResponse(cmd3);
        }

        private void PushReadingsIntoDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Add-TTReading -Database PowerShellTest.ptd -DataSource Temperature -Value 17.0 -TimeStamp ""10/07/2008 17:46""");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void ValidateDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDatabase -Database PowerShellTest.ptd");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);
            TimeSeriesDatabase database = returnedObjects[0].ImmediateBaseObject as TimeSeriesDatabase;
            Assert.That(database.Title, Is.EqualTo("PowerShell Unit Test Database"));
            Assert.That(database.StartTime, Is.EqualTo(new DateTime(2008, 7, 10, 17, 45, 0)));
            Assert.That(database.DataSources.Count, Is.EqualTo(4));
            ValidateDatabaseStats();
            ValidateTemperatureDataSource();
        }

        private void ValidateDatabaseStats()
        {
            DatabaseStats stats = GetDatabaseStats();
            Assert.That(stats.Total, Is.EqualTo(1));
            Assert.That(stats.Discarded, Is.EqualTo(0));
        }

        private void ValidateTemperatureDataSource()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataSource -Database PowerShellTest.ptd -DataSource temperature");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);
            DataSource dataSource = returnedObjects[0].ImmediateBaseObject as DataSource;
            Assert.That(dataSource.Name, Is.EqualTo("Temperature"));
            Assert.That(dataSource.PollingInterval, Is.EqualTo(new TimeSpan(0, 1, 0)));
            ValidateTemperatureAllArchive();
        }

        private void ValidateTemperatureAllArchive()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTArchive -Database PowerShellTest.ptd -DataSource Temperature -Archive all");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            Archive archive = returnedObjects[0].ImmediateBaseObject as Archive;
            Assert.That(archive.Name, Is.EqualTo("All"));
            Assert.That(archive.XFactor, Is.EqualTo(50));
            Assert.That(archive.ConsolidationFunction, Is.EqualTo(ArchiveTemplate.ConsolidationFunctionType.Average));
            Assert.That(archive.NumReadingsPerDataPoint, Is.EqualTo(1));
            Assert.That(archive.MaxDataPoints, Is.EqualTo(96));

            // Validate accumulated reading collection
            Assert.That(archive.AccumulatedReadings.Count, Is.EqualTo(0));

            ValidateAllArchiveDataPoints();
        }

        private void ValidateAllArchiveDataPoints()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataPoints -Database PowerShellTest.ptd -DataSource Temperature -Archive All");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            DataPoint[] dataPoints = ConvertReturnedObjectToDataPoints(returnedObjects);

            // Validate data point collection
            DataPoint[] expectedDataPoints = 
            {
                new DataPoint(17D, new DateTime(2008, 7, 10, 17, 46, 0))
            };

            Assert.That(dataPoints.Length, Is.EqualTo(expectedDataPoints.Length));
            Assert.That(dataPoints, Is.EqualTo(expectedDataPoints));
        }

        private void WriteErrorsToConsole(Collection<Object> errors)
        {
            foreach (Object o in errors)
            {
                Console.WriteLine(o.ToString());
            }
        }

        private void ValidatePipelineResponse(Pipeline cmd)
        {
            Collection<Object> errors = cmd.Error.ReadToEnd();
            WriteErrorsToConsole(errors);
            Assert.That(errors.Count, Is.EqualTo(0));
        }

        private void GenerateGraph()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"New-TTChart -Database PowerShellTest.ptd -DataSource Temperature -Archive All -Title ""A unit test"" -YAxisLabel ""Temperature (Centigrade)"" -XAxisLabel Time");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private DataPoint[] ConvertReturnedObjectToDataPoints(Collection<PSObject> powerShellObjects)
        {
            List<DataPoint> dataPoints = new List<DataPoint>();

            for (int i = 0; i < powerShellObjects.Count; i++)
            {
                DataPoint dataPoint = powerShellObjects[i].ImmediateBaseObject as DataPoint;
                dataPoints.Add(dataPoint);
            }

            return dataPoints.ToArray();
        }
    }
}
