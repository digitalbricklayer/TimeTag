namespace Openxtra.TimeTag.Stats.PowerShell
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Diagnostics;

    /// <summary>
    /// Gets the standard deviation of a set of numbers
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "StdDeviation")]
    public class GetStdDeviationCmdlet : PSCmdlet
    {
    	private List<double> sample = new List<double>();

        #region Parameters

        /// <summary>
        /// The numbers to be averaged
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The numbers to be averaged.")]
        [ValidateNotNullOrEmpty]
        public double[] Numbers
        {
            get { return this.numbers; }
            set { this.numbers = value; }
        }
        private double[] numbers;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Process each batch of numbers
        /// </summary>
        protected override void ProcessRecord()
        {
            Debug.Assert(numbers.Length > 0);

    		this.sample.AddRange(this.numbers);
        }

        /// <summary>
        /// Returns the arithmetic mean of all of the numbers
        /// </summary>
        protected override void EndProcessing()
        {
            WriteObject(Math.Sqrt(CalcVariance()));
        }

        #endregion Overrides

	    private double CalcVariance()
	    {
		    double average = CalcMean();
    		
		    double aggregate = 0D;
		    foreach (double s in this.sample)
		    {
                double deviation = s - average;
			    aggregate += deviation * deviation;
		    }
    		
		    return aggregate / this.sample.Count;
	    }

        private double CalcMean()
        {
            double aggregate = 0D;
            foreach (double s in this.sample)
            {
                aggregate += s;
            }

            return aggregate / this.sample.Count;
        }
    }
}
