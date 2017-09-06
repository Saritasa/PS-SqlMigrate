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
    $script:FullHistoryTableName = "[$script:DefaultSchema].[$script:HistoryTable]"
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
        [int]$Timeout = $script:DefaultCommandTimeout,
        [System.Data.SqlClient.SqlParameter[]]$CommandParameters
    )
    $ConnectionString = "$ConnectionString;Connection Timeout=$Timeout";
    $connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
    $connection.Open()
    
    try
    {
        $command = $connection.CreateCommand()
        $command.CommandText = $SqlCommand
        $command.CommandTimeout = $Timeout
        if ($CommandParameters -and $CommandParameters.Count -gt 0)
        {
            $command.Parameters.AddRange($CommandParameters)
        }
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

function Invoke-SQLFromFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
        [ValidateScript({ [System.IO.File]::Exists((Resolve-Path $_)) })]
        [string[]]$FilePath,
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString
    )

    Process {
        $fileName = (Get-Item $FilePath).BaseName
        $isDataFile = $fileName.EndsWith('.dat')
        
        $fileText = Get-Content $FilePath
        $commandExecutionTimeout = if ($isDataFile) { $script:DataMigrationTimeout } else { $script:DefaultCommandTimeout }
        $batchScript = $null
        $linePos = 0
        $lineBlockStart = 0
        Write-Information "Executing file $FilePath"
        try
        {
            # split the file in batch commands (separated by GO statements) and run them
            foreach ($line in $fileText)
            {
                if ($line -match '^\s*go\s*$')
                {
                    # exec
                    Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $batchScript -Timeout $commandExecutionTimeout
                    $batchScript = $null
                    $lineBlockStart = $linePos + 1
                }
                else
                {
                    if ($batchScript -ne $null)
                    {
                        $batchScript += "`n"
                    }
                    $batchScript += $line
                }

                ++$linePos
            }
            Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $batchScript -Timeout $commandExecutionTimeout
            Write-Information "File $FilePath executed successfully"
        }
        catch [System.Data.SqlClient.SqlException]
        {
            $OccurredException = $_.Exception.InnerException
            $ExceptionLine = $lineBlockStart + $OccurredException.LineNumber
            throw "Error executing file $($FilePath):
            $($OccurredException.Message)
            At line $ExceptionLine, Error Number: $($OccurredException.Number), State: $($OccurredException.State), Class: $($OccurredException.Class)"
        }
    }
}

function Invoke-MigrationsInFolder {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [string]$FolderPath,
        [switch]$Recurse
    )

    Initialize-HistoryTable -ConnectionString $ConnectionString

    $executedFilesCount = 0
    Get-ChildItem $FolderPath -File -Filter *.sql -Recurse:$Recurse |
        ForEach-Object {
            $fileName = $_.Name
            $executeScript = !(Test-FileMigrated -ConnectionString $ConnectionString -FileName $fileName)
            if ($executeScript)
            {
                Invoke-SQLFromFile -FilePath $_.FullName -ConnectionString $ConnectionString
                Set-FileMigrated -ConnectionString $ConnectionString -FileName $fileName
                ++$executedFilesCount
            }
        }
    Write-Information "Successfully executed $executedFilesCount migration scripts."
}

function Get-PatchHistoryTableName {
    $script:FullHistoryTableName
}

function Test-FileMigrated {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    $SearchCommand = "
If Exists (Select * From $script:FullHistoryTableName Where name = @PatchFileName)
    Select 1 As [Exists]
Else
    Select 0 As [Exists]
"
    $PatchNameParameter = New-Object System.Data.SqlClient.SqlParameter
    $PatchNameParameter.ParameterName = '@PatchFileName'
    $PatchNameParameter.Value = $FileName
    $fileMigrated = 0 -lt ((Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $SearchCommand -CommandParameters $PatchNameParameter).Exists)

    $fileMigrated
}

function Set-FileMigrated {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )
    $InsertCommand = "
If Not Exists (Select * From $script:FullHistoryTableName Where name = @PatchFileName)
    Insert $script:FullHistoryTableName(name, applied_at)
    Values (@PatchFileName, GetUTCDate())
"
    $PatchNameParameter = New-Object System.Data.SqlClient.SqlParameter
    $PatchNameParameter.ParameterName = '@PatchFileName'
    $PatchNameParameter.Value = $FileName
    Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $InsertCommand -CommandParameters $PatchNameParameter
}

function Initialize-HistoryTable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString
    )

    $TableSchemaParameter = New-Object System.Data.SqlClient.SqlParameter
    $TableSchemaParameter.ParameterName = '@TableSchema'
    $TableSchemaParameter.Value = $script:DefaultSchema
    $TableNameParameter = New-Object System.Data.SqlClient.SqlParameter
    $TableNameParameter.ParameterName = '@TableName'
    $TableNameParameter.Value = $script:HistoryTable
    $TableExists = (Invoke-SQL -ConnectionString $ConnectionString `
        -SqlCommand "Select Count(*) As [Count] From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA = @TableSchema And TABLE_NAME = @TableName" `
        -CommandParameters $TableSchemaParameter, $TableNameParameter ).Count `
            -gt 0

    if (!$TableExists)
    {
        Write-Verbose "Creating table $script:FullHistoryTableName"
        $TableDefinitionCommand = "
Create Table $script:FullHistoryTableName (
    id int Not Null Identity(1, 1),
    name varchar(100) Not Null,
    applied_at Datetime2 Not Null,
    Constraint [PK_$script:HistoryTable] Primary Key Clustered ( id ),
    Index [IX_$($script:HistoryTable)_Name] NonClustered (Name)
)"
        Invoke-SQL -ConnectionString $ConnectionString -SqlCommand $TableDefinitionCommand
    }
}

Set-Defaults -PatchHistoryTableName '_patch_history' -DatabaseSchema 'dbo' -DefaultCommandTimeout 60 -DataMigrationTimeout 600
