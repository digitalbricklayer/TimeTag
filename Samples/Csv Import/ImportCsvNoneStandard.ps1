# ImportCsv.ps1 - Import CSV file readings into a database
#

$currentDirectory = Convert-Path (Get-Location -PSProvider FileSystem)

$databasePath = join-path $currentDirectory "test.ptd"
$csvPath = join-path $currentDirectory "Temperatures.csv"

# Delete the database if it already exists
if (test-path $databasePath) {
	Delete-TTDatabase -Database $databasePath
}

# Create the database
New-TTDatabase -Database $databasePath -StartTime Now -Title "Temperature Database" -Force -DataSource  A:gauge:10:1:1000,B:gauge:10:1:1000,C:gauge:10:1:1000,D:gauge:10:1:1000 -Archive All:average:50:1:8640,Avg:average:50:8640:90

# Import the readings from a CSV file but rename the columns to correspond to match names expected by Add-TTReading
import-csv NoneStandardTemperatures.csv | select @{Name="Room";Expression={ $_.DataSource } },@{Name="Temperature";Expression={ $_.Value } },@{Name="Time";Expression={ $_.Timestamp } } | add-ttreading Temperatures.ptd
