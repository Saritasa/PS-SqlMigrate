$HistoryTable = '_patch_history'
$DefaultSchema = 'dbo'
$FullHistoryTableName = "[$DefaultSchema].[$HistoryTable]"

function Invoke-SQL {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [Parameter(Mandatory = $true)]
        [string]$sqlCommand,
        [int]$Timeout = 60
    )

    $ConnectionString = "$ConnectionString;Connection Timeout=$Timeout";
    $connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
    $connection.Open()
    
    try
    {
        $command = New-Object System.Data.SqlClient.SqlCommand($sqlCommand, $connection)
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
        $dataset = New-Object System.Data.DataSet
        $adapter.Fill($dataSet) | Out-Null
    }
    finally
    {
        $connection.Close()
    }
    
    $dataSet.Tables
}

function Get-PatchHistoryTableName {
    $FullHistoryTableName
}

function Initialize-HistoryTable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString
    )

    $TableExists = (Invoke-SQL -ConnectionString $ConnectionString `
        -sqlCommand "Select Count(*) As [Count] From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA = '$DefaultSchema' And TABLE_NAME = '$HistoryTable'").Count `
            -gt 0

    if (!$TableExists)
    {
        Write-Verbose "Creating table $FullHistoryTableName"
        $TableDefinitionCommand = "
Create Table $FullHistoryTableName (
    id int Not Null Identity(1, 1),
    name varchar(100) Not Null,
    applied_at Datetime2 Not Null,
    Constraint [PK_$HistoryTable] Primary Key Clustered ( id )
)"
        Invoke-Sql -ConnectionString $ConnectionString -sqlCommand $TableDefinitionCommand
    }
}
