namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Collections.Generic;
    using System.Security.AccessControl;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Text;
    using System.IO;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// This class implements the New-TTDatabase cmdlet.
    /// </summary>
    [Cmdlet(VerbsCommon.New, "TTDatabase",
        DefaultParameterSetName = "DBParameterSet")]
    public class NewDatabaseCmdlet : PSCmdlet
    {
        private static readonly string ConfigSeparator = ":";
        private TimeSeriesDatabaseTemplate template;

        #region Parameters

        /// <summary>
        /// The path and filename of the database file.
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage="The path and filename of the database to create.")]
        [ValidateNotNullOrEmpty]
        public string Database
        {
            get { return fileName; }
            set { fileName = value; }
        }
        private string fileName;

        /// <summary>
        /// The start time of the database.
        /// </summary>
        [Parameter(
            Position = 1,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The start time for the database.")]
        [ValidateNotNullOrEmpty]
        public string StartTime
        {
            get { return this.startTimeAsString; }
            set { this.startTimeAsString = value; }
        }
        private string startTimeAsString;

        /// <summary>
        /// DataSources to add to the database.
        /// </summary>
        [Parameter(
            Position = 2,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "DataSources to add to the database.")]
        [ValidateNotNullOrEmpty]
        public string[] DataSource
        {
            get { return dataSources; }
            set { dataSources = value; }
        }
        private string[] dataSources;

        /// <summary>
        /// Archives to add to each DataSource in the database.
        /// </summary>
        [Parameter(
            Position = 3,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "Archives to add to each DataSource in the database.")]
        [ValidateNotNullOrEmpty]
        public string[] Archive
        {
            get { return archives; }
            set { archives = value; }
        }
        private string[] archives;

        /// <summary>
        /// Overwrite existing file.
        /// </summary>
        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "Overwrite existing file.")]
        public SwitchParameter Force
        {
            get { return this.force; }
            set { this.force = value; }
        }
        private bool force;

        /// <summary>
        /// Gets the database title
        /// </summary>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The title to give the TimeTag database.")]
        public string Title
        {
            get { return this.title; }
            set { this.title = value; }
        }
        private string title;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Create the TimeTag TimeSeries Database.
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.Database);

            WriteDebug("Extracting DateTime object from string: " + this.startTimeAsString);
            DateTime startTime = DateTimeHelper.CreateDateTimeFromString(this.startTimeAsString);

            if (!Force && File.Exists(databasePath))
            {
                WriteError(
                    new ErrorRecord(
                        new IOException("TimeTag database file already exists."),
                        "FileExists",
                        ErrorCategory.WriteError,
                        this.Database));
                return;
            }

            this.template = new TimeSeriesDatabaseTemplate();
            this.template.StartTime = startTime;
            this.template.Title = this.Title;

            this.template.Title = this.Title ?? String.Empty;

            foreach (string dataSourceConfigAsString in this.DataSource)
            {
                if (!AddDataSourceToTemplate(dataSourceConfigAsString))
                    return;
            }

            foreach (string archiveTemplateAsString in this.Archive)
            {
                if (!AddArchiveToTemplate(archiveTemplateAsString))
                    return;
            }

            WriteVerbose(
                String.Format("Creating TimeSeries Database using\nDBFilePath: {0}\nDBStartTime: {1}", 
                this.Database, startTime));

            TimeSeriesDatabase database = TimeSeriesDatabase.Create(databasePath, this.template);
            database.Close();
        }

        #endregion Overrides

        #region Helper Methods

        private bool AddDataSourceToTemplate(string dataSourceConfig)
        {
            string[] configChunk = dataSourceConfig.Split(ConfigSeparator.ToCharArray());

            if (configChunk.Length != 5)
            {
                WriteError(
                    new ErrorRecord(
                        new ArgumentException(),
                        "InvalidDataSourceConfig",
                        ErrorCategory.InvalidArgument,
                        dataSourceConfig));

                return false;
            }
            else
            {
                string name = configChunk[0];

                string conversionFunctionAsString = configChunk[1].ToUpper();
                if (!ValidateConversionFunction(conversionFunctionAsString))
                {
                    WriteError(
                        new ErrorRecord(
                            new ArgumentException("Unknown data source type"),
                            "InvalidDataSourceConfig",
                            ErrorCategory.InvalidArgument,
                            this.Database));
                    return false;
                }

                int intervalInSeconds;

                if (!SafeConvertInt32(out intervalInSeconds, configChunk[2]))
                {
                    WriteError(
                        new ErrorRecord(
                            new ArgumentException("Invalid poll interval"),
                            "InvalidDataSourceConfig",
                            ErrorCategory.InvalidArgument,
                            this.Database));
                    return false;
                }

                double min;

                if (!SafeConvertDouble(out min, configChunk[3]))
                {
                    WriteError(
                        new ErrorRecord(
                            new ArgumentException("Invalid min threshold"),
                            "InvalidDataSourceConfig",
                            ErrorCategory.InvalidArgument,
                            this.Database));
                    return false;
                }

                double max;

                if (!SafeConvertDouble(out max, configChunk[4]))
                {
                    WriteError(
                        new ErrorRecord(
                            new ArgumentException("Invalid max threshold"),
                            "InvalidDataSourceConfig",
                            ErrorCategory.InvalidArgument,
                            this.Database));
                    return false;
                }

                this.template.AddDataSource(
                    name, ConvertConversionFunction(conversionFunctionAsString), new TimeSpan(0, 0, intervalInSeconds), min, max);
            }

            return true;
        }

        private bool AddArchiveToTemplate(string archiveConfig)
        {
            string[] configChunk = archiveConfig.Split(ConfigSeparator.ToCharArray());

            if (configChunk.Length != 5)
            {
                WriteError(
                    new ErrorRecord(
                        new ArgumentException(),
                        "InvalidArchiveConfig",
                        ErrorCategory.InvalidArgument,
                        archiveConfig));

                return false;
            }
            else
            {
                string name = configChunk[0];

                string consolidationFunctionAsString = configChunk[1].ToUpper();

                if (!ValidateConsolidationFunction(consolidationFunctionAsString))
                    return false;

                int xFactor;

                if (!SafeConvertInt32(out xFactor, configChunk[2]))
                    return false;

                int readingsPerDataPoint;

                if (!SafeConvertInt32(out readingsPerDataPoint, configChunk[3]))
                    return false;

                int numDataPoint;

                if (!SafeConvertInt32(out numDataPoint, configChunk[4]))
                    return false;

                this.template.AddArchive(
                    name, ConvertConsolidationFunction(consolidationFunctionAsString), xFactor, readingsPerDataPoint, numDataPoint);
            }

            return true;
        }

        private Openxtra.TimeTag.Database.DataSource.ConversionFunctionType ConvertConversionFunction(string conversionFunctionAsString)
        {
            if (conversionFunctionAsString.CompareTo("GAUGE") == 0)
            {
                return Openxtra.TimeTag.Database.DataSource.ConversionFunctionType.Gauge;
            }
            else if (conversionFunctionAsString.CompareTo("COUNTER") == 0)
            {
                return Openxtra.TimeTag.Database.DataSource.ConversionFunctionType.Counter;
            }
            else if (conversionFunctionAsString.CompareTo("DERIVE") == 0)
            {
                return Openxtra.TimeTag.Database.DataSource.ConversionFunctionType.Derive;
            }
            else if (conversionFunctionAsString.CompareTo("ABSOLUTE") == 0)
            {
                return Openxtra.TimeTag.Database.DataSource.ConversionFunctionType.Absolute;
            }
            else
            {
                throw new ArgumentException("Invalid conversion function");
            }
        }

        private Openxtra.TimeTag.Database.ArchiveTemplate.ConsolidationFunctionType ConvertConsolidationFunction(string consolidationFunctionAsString)
        {
            if (consolidationFunctionAsString.CompareTo("LAST") == 0)
            {
                return Openxtra.TimeTag.Database.ArchiveTemplate.ConsolidationFunctionType.Last;
            }
            else if (consolidationFunctionAsString.CompareTo("AVERAGE") == 0)
            {
                return Openxtra.TimeTag.Database.ArchiveTemplate.ConsolidationFunctionType.Average;
            }
            else if (consolidationFunctionAsString.CompareTo("MIN") == 0)
            {
                return Openxtra.TimeTag.Database.ArchiveTemplate.ConsolidationFunctionType.Min;
            }
            else if (consolidationFunctionAsString.CompareTo("MAX") == 0)
            {
                return Openxtra.TimeTag.Database.ArchiveTemplate.ConsolidationFunctionType.Max;
            }
            else
            {
                throw new ArgumentException("Invalid consolidation function");
            }
        }

        /// <summary>
        /// Check the specified DataSource type is valid.
        /// </summary>
        /// <param name="dataSourceType">String representing the DataSource type.</param>
        /// <returns>False if the DataSource type in invalid.</returns>
        private bool ValidateConversionFunction(string dataSourceType)
        {
            StringComparer comparer = StringComparer.Create(
                System.Globalization.CultureInfo.CurrentCulture, true);

            if (comparer.Compare(dataSourceType, "GAUGE") == 0)
                return true;
            else if (comparer.Compare(dataSourceType, "COUNTER") == 0)
                return true;
            else if (comparer.Compare(dataSourceType, "DERIVE") == 0)
                return true;
            else if (comparer.Compare(dataSourceType, "ABSOLUTE") == 0)
                return true;
            else
                return false;
        } // CheckADataSourceType

        /// <summary>
        /// Check the specified Archive type is valid.
        /// </summary>
        /// <param name="archiveType">String representing the Archive type.</param>
        /// <returns>False if the Archive type in invalid.</returns>
        private bool ValidateConsolidationFunction(string archiveType)
        {
            StringComparer comparer = StringComparer.Create(
                System.Globalization.CultureInfo.CurrentCulture,
                true);

            if (comparer.Compare(archiveType, "AVERAGE") == 0)
                return true;
            else if (comparer.Compare(archiveType, "MIN") == 0)
                return true;
            else if (comparer.Compare(archiveType, "MAX") == 0)
                return true;
            else if (comparer.Compare(archiveType, "LAST") == 0)
                return true;
            else
                return false;

        } // CheckArchiveType

        /// <summary>
        /// Method to safely convert a string representation of a number 
        /// into its Int32 equivalent
        /// </summary>
        /// <param name="number">The converted number</param>
        /// <param name="numberAsStr">String representation of the number</param>
        /// <returns>False if there is an exception.</returns>
        private bool SafeConvertInt32(out int number, string numberAsStr)
        {
            number = -1;
            try
            {
                number = Convert.ToInt32(numberAsStr);
                return true;
            }
            catch (FormatException fe)
            {
                WriteError(new ErrorRecord(fe, "FormatNotValid",
                    ErrorCategory.InvalidData, numberAsStr));
            }
            catch (OverflowException oe)
            {
                WriteError(new ErrorRecord(oe, "Overflow",
                    ErrorCategory.InvalidData, numberAsStr));
            }

            return false;
        } // SafeConvertInt32

        /// <summary>
        /// Method to safely convert a string representation of a number 
        /// into its Double equivalent
        /// </summary>
        /// <param name="number">The converted number</param>
        /// <param name="numberAsStr">String representation of the number</param>
        /// <returns>False if there is an exception.</returns>
        private bool SafeConvertDouble(out double number, string numberAsStr)
        {
            number = double.NaN;
            try
            {
                number = Convert.ToDouble(numberAsStr);
                return true;
            }
            catch (FormatException fe)
            {
                WriteError(new ErrorRecord(fe, "FormatNotValid",
                    ErrorCategory.InvalidData, numberAsStr));
            }
            catch (OverflowException oe)
            {
                WriteError(new ErrorRecord(oe, "Overflow",
                    ErrorCategory.InvalidData, numberAsStr));
            }

            return false;
        } // SafeConvertDouble

        #endregion Helper Methods
    }
}
