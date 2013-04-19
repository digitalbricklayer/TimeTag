namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.IO;
    using System.Diagnostics;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Delete a TimeTag database
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "TTDatabase",
        DefaultParameterSetName = "DBParameterSet")]
    public class DeleteDatabaseCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;

        #region Parameters

        /// <summary>
        /// The database filename
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The database filename")]
        [ValidateNotNullOrEmpty]
        public string Database
        {
            get { return this.fileName; }
            set { this.fileName = value; }
        }
        private string fileName;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Returns the database in the specified file
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.fileName);

            if (!File.Exists(this.fileName))
            {
                WriteError(
                    new ErrorRecord(
                        new FileNotFoundException("TimeTag database file not found."),
                        "FileNotFound",
                        ErrorCategory.WriteError,
                        this.fileName));
                return;
            }

            try
            {
                this.database = TimeSeriesDatabase.Read(databasePath);
                this.database.Delete();
            }
            catch (Exception e)
            {
                WriteError(
                    new ErrorRecord(
                       new ArgumentException("Unknown error occured"),
                       "UnknownError", ErrorCategory.InvalidArgument, e.ToString()));
            }
        }

        #endregion Overrides
    }
}
