namespace Openxtra.TimeTag.Stats.PowerShell.Test
{
    using System;
    using System.IO;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;
    using System.Management.Automation.Runspaces;
    using System.Management.Automation;
    using NUnit.Framework;
    using NUnit.Framework.SyntaxHelpers;

    /// <summary>
    /// Fixture for testing the TimeTag Stats PowerShell snap-in
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
            this.config.AddPSSnapIn("PowerStatsPowerShellSnapIn", out warning);
            this.config.AddPSSnapIn("TimeTagPowerShellSnapIn", out warning);
            this.runspace = RunspaceFactory.CreateRunspace(this.config);
            this.runspace.Open();
        }

        [TearDown]
        protected void TearDown()
        {
            this.runspace.Close();
        }

        [Test]
        public void TestMean1()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Mean -Number 1,2,3,4,5,6,7,8,9");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double mean = (Double) returnedObjects[0].ImmediateBaseObject;
            Assert.That(mean, Is.EqualTo(5D));
        }

        [Test]
        public void TestMean2()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Mean -Number 1");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double mean = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(mean, Is.EqualTo(1D));
        }

        [Test]
        public void TestMean3()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Mean -Number 0");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double mean = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(mean, Is.EqualTo(0D));
        }

        [Test]
        [ExpectedException(typeof(ParameterBindingException))]
        public void TestMean4()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Mean -Number");
            Collection<PSObject> returnedObjects = cmd.Invoke();
        }

        [Test]
        public void TestMean5()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"1,2,3 | Get-Mean");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double mean = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(mean, Is.EqualTo(2D));
        }

        [Test]
        public void TestMean6()
        {
            CreateDatabase();
            PushReadingsIntoDatabase();
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-TTDataPoints -Database PowerStatsTest.ptd -DataSource Temperature -Archive All | Get-Mean");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double mean = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(mean, Is.EqualTo(2D));
            DeleteDatabase();
        }

        [Test]
        public void TestMedian1()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Median -Number 1,2,3,4,5,6,7,8,9");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(5D));
        }

        [Test]
        public void TestMedian2()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Median -Number 1,2,3,4,5,6,7,8");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(4.5D));
        }

        [Test]
        public void TestMedian3()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"3,2,1 | Get-Median");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(2D));
        }

        [Test]
        [ExpectedException(typeof(ParameterBindingException))]
        public void TestMedian4()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-Median -Number");
            Collection<PSObject> returnedObjects = cmd.Invoke();
        }

        [Test]
        public void TestMedian5()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"5 | Get-Median");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(5D));
        }

        [Test]
        public void TestStdDeviation1()
        {
            // Example taken from Statistics without Tears by Derek Rowntree Pg. 54 ex. b
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-StdDeviation -Number 111,114,117,118,120");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(3.16227766D).Within(.00000001));
        }

        [Test]
        public void TestStdDeviation2()
        {
            // Example taken from Statistics without Tears by Derek Rowntree Pg. 54 ex. a
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-StdDeviation -Number 6,24,37,49,64");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(19.98999749D).Within(.00000001));
        }

        [Test]
        [ExpectedException(typeof(ParameterBindingException))]
        public void TestStdDeviation3()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Get-StdDeviation -Number");
            Collection<PSObject> returnedObjects = cmd.Invoke();
        }

        [Test]
        public void TestStdDeviation4()
        {
            // Example taken from Statistics without Tears by Derek Rowntree Pg. 54 ex. a
            Pipeline cmd = this.runspace.CreatePipeline(@"6,24,37,49,64 | Get-StdDeviation");
            Collection<PSObject> returnedObjects = cmd.Invoke();
            ValidatePipelineResponse(cmd);

            double median = (Double)returnedObjects[0].ImmediateBaseObject;
            Assert.That(median, Is.EqualTo(19.98999749D).Within(.00000001));
        }

        private void WriteErrorsToConsole(Collection<Object> errors)
        {
            foreach (Object e in errors)
            {
                Console.WriteLine(e.ToString());
            }
        }

        private void ValidatePipelineResponse(Pipeline cmd)
        {
            Collection<Object> errors = cmd.Error.ReadToEnd();
            WriteErrorsToConsole(errors);
            Assert.That(errors.Count, Is.EqualTo(0));
        }

        private void CreateDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"New-TTDatabase -Database PowerStatsTest.ptd -StartTime ""10/07/2008 17:45"" -Title ""PowerShell Unit Test Database"" -Force -DataSource Temperature:gauge:60:0:100 -Archive All:average:50:1:96");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void DeleteDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Delete-TTDatabase -Database PowerStatsTest.ptd");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }

        private void PushReadingsIntoDatabase()
        {
            Pipeline cmd = this.runspace.CreatePipeline(@"Add-TTReading -Database PowerStatsTest.ptd -DataSource Temperature -Value 1,2,3 -TimeStamp ""10/07/2008 17:46"",""10/07/2008 17:47"",""10/07/2008 17:48""");
            cmd.Invoke();
            ValidatePipelineResponse(cmd);
        }
    }
}
