# SimpleReadLoop.ps1 - Simple example of squirting readings into a database
#

$currentDirectory = Convert-Path (Get-Location -PSProvider FileSystem)

$path = join-path $currentDirectory "test.ptd"

# Delete the database if it already exists
if (test-path $path) {
	Delete-TTDatabase -Database $path
}

# Create the database
New-TTDatabase -Database $path -StartTime Now -Title "Sample Loop" -Force -DataSource A:gauge:10:1:1000,B:gauge:10:1:1000,C:gauge:10:1:1000,D:gauge:10:1:1000 -Archive All:average:50:1:8640,Avg:average:50:8640:90

# Squirt some dummy data into the database
foreach ($x in 1..10) {
	# wait for the polling interval
	start-sleep 10
	Add-TTReading -Database $path -DataSource A,B,C,D -Value $x,$x,$x,$x
}
