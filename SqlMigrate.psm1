[string]$script:HistoryTable = $null
[string]$script:DefaultSchema = $null
[string]$script:FullHistoryTableName = $null
[int]$script:DefaultCommandTimeout = $null
[int]$script:DataMigrationTimeout = $null

function Set-Defaults {
    [CmdletBinding()]
    param (
        [string]$PatchHistoryTableName,
        [string]$DatabaseSchema,
        [int]$DefaultCommandTimeout,
        [int]$DataMigrationTimeout
    )

    if ($PatchHistoryTableName -ne $null)
    {
        Write-Verbose "Setting patch history table name to $PatchHistoryTableName"
        $script:HistoryTable = $PatchHistoryTableName
    }

    if ($DatabaseSchema -ne $null)
    {
        Write-Verbose "Setting used database schema to $DatabaseSchema"
        $script:DefaultSchema = $DatabaseSchema
    }
    $script:FullHistoryTableName = "[$($script:DefaultSchema)].[$($script:HistoryTable)]"
    Write-Verbose "Full patch history table name is $script:FullHistoryTableName"

    if ($DefaultCommandTimeout -ne $null)
    {
        Write-Verbose "Setting default command timeout to $DefaultCommandTimeout seconds"
        $script:DefaultCommandTimeout = $DefaultCommandTimeout
    }

    if ($DataMigrationTimeout -ne $null)
    {
        Write-Verbose "Setting default timeout for data migrations to $DataMigrationTimeout seconds"
        $script:DataMigrationTimeout = $DataMigrationTimeout
    }
}

function Invoke-SQL {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [Parameter(Mandatory = $true)]
        [string]$SqlCommand,
        [int]$Timeout = $script:DefaultCommandTimeout
    )

    $ConnectionString = "$ConnectionString;Connection Timeout=$Timeout";
    $connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
    $connection.Open()
    
    try
    {
        $command = New-Object System.Data.SqlClient.SqlCommand($SqlCommand, $connection)
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
        $dataset = New-Object System.Data.DataSet
        $adapter.Fill($dataSet) | Out-Null
    }
    catch
    {
        throw
    }
    finally
    {
        $connection.Close()
    }
    
    $dataSet.Tables
}

function Invoke-SQLFromFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
        [ValidateScript({ [System.IO.File]::Exists($_) })]
        [string[]]$FilePath,
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString
    )

    Process {
        $fileName = (Get-Item $FilePath).BaseName
        $isDataFile = $fileName.EndsWith('.dat')
        
        $fileText = Get-Content $FilePath -Raw
        $commandExecutionTimeout = if ($isDataFile) { $script:DataMigrationTimeout } else { $script:DefaultCommandTimeout }
        Write-Information "Executing file $FilePath"
        Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $fileText -Timeout $commandExecutionTimeout
        Write-Information "File $FilePath executed successfully"
    }
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
        -SqlCommand "Select Count(*) As [Count] From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA = '$DefaultSchema' And TABLE_NAME = '$HistoryTable'").Count `
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
        Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $TableDefinitionCommand
    }
}


Set-Defaults -PatchHistoryTableName '_patch_history' -DatabaseSchema 'dbo' -DefaultCommandTimeout 60 -DataMigrationTimeout 600
