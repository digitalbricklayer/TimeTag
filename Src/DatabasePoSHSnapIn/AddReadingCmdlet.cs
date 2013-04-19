namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Management.Automation;
    using System.IO;
    using System.Diagnostics;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Cmdlet for adding a reading to a time series database
    /// </summary>
    [Cmdlet(VerbsCommon.Add, "TTReading",
        DefaultParameterSetName = "DBParameterSet")]
    public class AddReadingCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;

        #region Parameters

        /// <summary>
        /// Gets the database to add the reading to
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The path and filename of the database to create.")]
        [ValidateNotNullOrEmpty]
        public string Database
        {
            get { return this.dbFileName; }
            set { this.dbFileName = value; }
        }
        private string dbFileName;

        /// <summary>
        /// The name of the datasource to add the reading to
        /// </summary>
        /// <remarks>This is case sensative.</remarks>
        [Parameter(
            Position = 1,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true,
            HelpMessage = "The name of the datasource (case sensitive) to add the reading to.")]
        [ValidateNotNullOrEmpty]
        public string[] DataSource
        {
            get { return this.dataSourceNames; }
            set { this.dataSourceNames = value; }
        }
        private string[] dataSourceNames;

        /// <summary>
        /// The value of the reading
        /// </summary>
        [Parameter(
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true,
            HelpMessage = "The value of the reading")]
        [ValidateNotNullOrEmpty]
        public double[] Value
        {
            get { return this.values; }
            set { this.values = value; }
        }
        private double[] values;

        /// <summary>
        /// The timestamp of the reading
        /// </summary>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true,
            HelpMessage = "The timestamp of the reading")]
        [ValidateNotNullOrEmpty]
        public string[] Timestamp
        {
            get { return this.timestamps; }
            set { this.timestamps = value; }
        }
        private string[] timestamps;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Add one or more readings to the specified data source
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.dbFileName);

            if (!File.Exists(databasePath))
            {
                WriteError(
                    new ErrorRecord(
                        new FileNotFoundException("TimeTag database file not found."),
                        "FileNotFound",
                        ErrorCategory.WriteError,
                        this.dbFileName));

                return;
            }

            try
            {
                this.database = TimeSeriesDatabase.Read(
                    databasePath, TimeSeriesDatabase.ConnectionMode.ReadWrite);

                string dataSourceName = String.Empty;

                for (int index = 0; index < this.values.Length; index++)
                {
                    /*
                     * Where a data source name is not specified for each 
                     * value add the readings to the last specified data source
                     */
                    if (index < this.dataSourceNames.Length)
                    {
                        dataSourceName = (string)this.dataSourceNames.GetValue(index);
                    }
                    double value = (double)this.values.GetValue(index);

                    DateTime timestamp = DateTime.Now;

                    // Timestamps are optional, when absent assume "Now" is the argument
                    if (this.timestamps != null && this.timestamps.Length >= index)
                    {
                        timestamp = DateTimeHelper.CreateDateTimeFromString(
                            (string)this.timestamps.GetValue(index));
                    }

                    Debug.Assert(dataSourceName != String.Empty);

                    // Add the reading to the database
                    this.database.Push(dataSourceName, new Reading(value, timestamp));
                }
            }
            catch (Exception e)
            {
                WriteError(
                    new ErrorRecord(
                        new Exception("TimeTag unexpected error."),
                        "Unexpected",
                        ErrorCategory.WriteError, e));
            }
            finally
            {
                this.database.Close();
            }
        }

        #endregion Overrides
    }
}
