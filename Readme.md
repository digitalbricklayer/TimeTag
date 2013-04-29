TimeTag 1.1 Alpha 1 Readme
==========================

*Please note: TimeTag is no longer under active development. Please feel free to fork.* If you want to know why, please read [Lessons learnt from the failure of TimeTag](http://www.openxtra.co.uk/blog/lessons-learnt-from-the-failure-of-timetag/).

Introduction
------------

TimeTag is a time series database implemented in C# for the PowerShell and .NET environments. The idea is
you throw numbers and associated timestamps at it and it stores and consolidates them and in an efficient way.

The TimeTag website can be found here: http://github.com/openxtra/TimeTag

Release
-------

Whilst the unit tests suggest TimeTag works I wouldn't use it for anything other than messing 
around with at the moment, mainly because of the lack of "in the field" usage. Until people 
are using TimeTag in the field and are having success using it I would hesitate to recommend 
it for "live" use.

Licence
-------

TimeTag is licenced under the General Public License v3. See http://www.gnu.org/licenses/gpl.html

Installation Instructions
-------------------------

For PowerShell installation instructions see PowerShell.txt in the same folder as this file.

Tests
-----

Unit tests have been created for the PowerShell functionality and for the core library. Both sets of unit tests 
use NUnit as the test framework.

The EcadPrecipitationFixture uses the whole ECAD precipitation data set, that is all of the precipitation data
from over a thousand weather stations. The ECAD fixure is marked as explicit so you need to specifically select 
it in order to run it.

The ECAD test data set can be downloaded as a seperate item to keep the download as light as possible. If you want to
run the ECAD unit test you will need to download the ECAD dataset from the http://code.google.com/p/powertime/ 
website.

All tests are in the TimeTag Tests folder. Code coverage for the Openxtra.TimeTag.Core namespace is 92%.

TODO
----

Outstanding issues can be found here: http://github.com/openxtra/TimeTag

Release History
---------------

See http://github.com/openxtra/TimeTag

Source layout
-------------

TimeTag
- bin
-- Debug
-- Release
-- utils
- Core
-- Properties
- Test
-- Core
--- Properties
-- PowerShellTest
--- Properties
-- Test Datasets
- PowerShell
-- Properties
- TimeTagSetup
- inc

Contact
-------

Contact the authors, Jack Hughes & Dean Sykes, via the TimeTag website at http://github.com/openxtra/TimeTag

Enjoy!
