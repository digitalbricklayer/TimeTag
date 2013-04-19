namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Security.AccessControl;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.IO;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Implements the export-pttimeseriesdb cmdlet
    /// 
    /// </summary>
    [Cmdlet("Export", "TTDatabase",
        DefaultParameterSetName = "DBParameterSet")]
    public class ExportDatabaseCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;
        private readonly string pathSeparator = "\\";

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
        /// Export file path and filename
        /// </summary>
        [Parameter(
            Position = 1,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The path and filename of the import file")]
        [ValidateNotNullOrEmpty]
        public string ExportFile
        {
            get { return this.exportFile; }
            set { this.exportFile = value; }
        }
        private string exportFile;

        /// <summary>
        /// Overwrite existing export file
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
        /// Export the TimeTag database to a file
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.fileName);

            if (!File.Exists(this.Database))
            {
                WriteError(
                    new ErrorRecord(
                        new IOException("TimeTag database doesn't exist"),
                        "FileMissing",
                        ErrorCategory.WriteError,
                        this.Database));
                return;
            }
            else if (!Force && File.Exists(this.exportFile))
            {
                WriteError(
                    new ErrorRecord(
                        new IOException("Export file already exists"),
                        "FileExists",
                        ErrorCategory.WriteError,
                        this.exportFile));
                return;
            }

            WriteVerbose(
                String.Format("Exporting database to file: {0}", this.exportFile));

            try
            {
                this.database = TimeSeriesDatabase.Read(databasePath);
                this.database.Export(this.exportFile);
            }
            finally
            {
                this.database.Close();
            }
        }

        /// <summary>
        /// This method makes sure that the correct path separator character is used.
        /// </summary>
        /// <param name="path"></param>
        /// <returns>normalized path</returns>
        private string NormalizePath(string path)
        {
            string result = path;

            if (!String.IsNullOrEmpty(path))
                result = path.Replace("/", pathSeparator);

            return result;
        }
    }
}
