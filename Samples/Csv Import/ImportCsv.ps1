# ImportCsv.ps1 - Import CSV file readings into a database
#

$currentDirectory = Convert-Path (Get-Location -PSProvider FileSystem)

$databasePath = join-path $currentDirectory "test.ptd"
$csvPath =  join-path $currentDirectory "Temperatures.csv"

# Delete the database if it already exists
if (test-path $databasePath) {
	Delete-TTDatabase -Database $databasePath
}

# Create the database
New-TTDatabase -Database $databasePath -StartTime Now -Title "Simple CSV Import" -Force -DataSource A:gauge:10:1:1000,B:gauge:10:1:1000,C:gauge:10:1:1000,D:gauge:10:1:1000 -Archive All:average:50:1:8640,Avg:average:50:8640:90

# Import the contents of the CSV file into the database
import-csv $csvPath | add-ttreading $databasePath
