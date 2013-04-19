namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Security.AccessControl;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.IO;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Implements the import-pttimeseriesdb cmdlet
    /// </summary>
    [Cmdlet("Import", "TTDatabase",
        DefaultParameterSetName = "DBParameterSet")]
    public class ImportDatabaseCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;

        /// <summary>
        /// The path and filename of the database file
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage="The path and filename of the database to create")]
        [ValidateNotNullOrEmpty]
        public string Database
        {
            get { return fileName; }
            set { fileName = value; }
        }
        private string fileName;

        /// <summary>
        /// Import file path and filename
        /// </summary>
        [Parameter(
            Position = 1,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The path and filename of the import file")]
        [ValidateNotNullOrEmpty]
        public string ImportFile
        {
            get { return this.importFile; }
            set { this.importFile = value; }
        }
        private string importFile;

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
        /// Import the TimeTag database from a previously exported file
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.Database);

            if (!Force && File.Exists(databasePath))
            {
                WriteError(
                    new ErrorRecord(
                        new IOException("TimeTag database already exists"),
                        "FileExists",
                        ErrorCategory.WriteError,
                        this.Database));
                return;
            }
            else if (File.Exists(this.importFile) != true)
            {
                WriteError(
                    new ErrorRecord(
                        new IOException("Import file does not exist"),
                        "MissingImportFile",
                        ErrorCategory.WriteError,
                        this.importFile));
                return;
            }

            WriteVerbose(
                String.Format("Importing database from file: {0}", this.importFile));

            try
            {
                this.database = TimeSeriesDatabase.Import(databasePath, this.importFile);
            }
            finally
            {
                this.database.Close();
            }
        }
    }
}
