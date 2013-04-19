namespace Openxtra.TimeTag.Stats.PowerShell
{
    using System;
    using System.Collections.ObjectModel;
    using System.Management.Automation;
    using System.Management.Automation.Runspaces;
    using System.ComponentModel;

    /// <summary>
    /// The TimeTag Stats PowerShell snap-in class
    /// </summary>
    [RunInstaller(true)]
    public class PowerShellSnapIn : CustomPSSnapIn
    {
        /// <summary>
        /// Creates an instance of the PowerStats PowerShell snap-in
        /// </summary>
        public PowerShellSnapIn()
            : base()
        {
        }

        /// <summary>
        /// Gets the name for this PowerShell snap-in. This name will be used in registering
        /// this PowerShell snap-in.
        /// </summary>
        public override string Name
        {
            get
            {
                return "Openxtra.TimeTag.Stats";
            }
        }

        /// <summary>
        /// Gets the vendor information for this PowerShell snap-in
        /// </summary>
        public override string Vendor
        {
            get
            {
                return "OPENXTRA Limited";
            }
        }

        /// <summary>
        /// Gets resource information for vendor. This is a string of format: 
        /// resourceBaseName,resourceName. 
        /// </summary>
        public override string VendorResource
        {
            get
            {
                return this.Name + "," + this.Vendor;
            }
        }

        /// <summary>
        /// Gets the description of the TimeTag PowerShell snap-in.
        /// </summary>
        public override string Description
        {
            get
            {
                return "This is a PowerShell snap-in for TimeTag Stats.";
            }
        }

        private Collection<CmdletConfigurationEntry> cmdlets;

        /// <summary>
        /// Gets the TimeTag cmdlets
        /// </summary>
        public override Collection<CmdletConfigurationEntry> Cmdlets
        {
            get
            {
                if (this.cmdlets == null)
                {
                    this.cmdlets = new Collection<CmdletConfigurationEntry>();

                    string cmdletHelpFile = "Openxtra.TimeTag.Stats.PowerShell.dll-Help.xml";

                    CmdletConfigurationEntry[] cmdletConfigs =
                    {
                        new CmdletConfigurationEntry("Get-Mean",        typeof(GetMeanCmdlet),          cmdletHelpFile),
                        new CmdletConfigurationEntry("Get-Median",      typeof(GetMedianCmdlet),        cmdletHelpFile),
                        new CmdletConfigurationEntry("Get-StdDeviation",typeof(GetStdDeviationCmdlet),  cmdletHelpFile),
                    };

                    foreach (CmdletConfigurationEntry entry in cmdletConfigs)
                    {
                        this.cmdlets.Add(entry);
                    }
                }

                return this.cmdlets;
            }
        }

        Collection<ProviderConfigurationEntry> providers;

        public override Collection<ProviderConfigurationEntry> Providers
        {
            get
            {
                if (this.providers == null)
                {
                    this.providers = new Collection<ProviderConfigurationEntry>();
                }

                return this.providers;
            }
        }

        private Collection<TypeConfigurationEntry> types;

        /// <summary>
        /// Gets the TimeTag PowerShell types
        /// </summary>
        public override Collection<TypeConfigurationEntry> Types
        {
            get
            {
                if (this.types == null)
                {
                    this.types = new Collection<TypeConfigurationEntry>();
                }

                return this.types;
            }
        }

        private Collection<FormatConfigurationEntry> formats;

        /// <summary>
        /// Gets the TimeTag formats
        /// </summary>
        public override Collection<FormatConfigurationEntry> Formats
        {
            get
            {
                if (this.formats == null)
                {
                    this.formats = new Collection<FormatConfigurationEntry>();
                }

                return this.formats;
            }
        }
    }
}
