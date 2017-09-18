[string]$script:HistoryTable = $null
[string]$script:DefaultSchema = $null
[string]$script:FullHistoryTableName = $null
[string]$script:CommandTerminator = $null
[int]$script:DefaultCommandTimeout = $null
[int]$script:DataMigrationTimeout = $null
[string]$script:ArtifactNameLinePrefix = '--require:'
[string]$script:TableNameLinePrefix = '--table:'

function Set-Defaults {
    [CmdletBinding()]
    param (
        [string]$PatchHistoryTableName,
        [string]$DatabaseSchema,
        [int]$DefaultCommandTimeout,
        [int]$DataMigrationTimeout,
        [string]$CommandTerminator
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

    if ($CommandTerminator -ne $null)
    {
        Write-Verbose "Setting command terminator to $CommandTerminator"
        $script:CommandTerminator = $CommandTerminator
    }

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
        [Parameter(Mandatory = $true, ParameterSetName = 'ConnectionString')]
        [string]$ConnectionString,
        [Parameter(Mandatory = $true, ParameterSetName = 'OpenConnection')]
        [Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory = $true)]
        [string]$SqlCommand,
        [int]$Timeout = $script:DefaultCommandTimeout,
        [Data.SqlClient.SqlParameter[]]$CommandParameters
    )
    if ($PSCmdlet.ParameterSetName -eq 'ConnectionString')
    {
        $ConnectionString = "$ConnectionString;Connection Timeout=$Timeout";
        $Connection = New-Object Data.SqlClient.SqlConnection($ConnectionString)
    }

    try
    {
        if ($Connection.State -ne 'Open')
        {
            $Connection.Open()
        }

        $command = $Connection.CreateCommand()
        $command.CommandText = $SqlCommand
        $command.CommandTimeout = $Timeout
        if ($CommandParameters -and $CommandParameters.Count -gt 0)
        {
            $command.Parameters.AddRange($CommandParameters)
        }
        $adapter = New-Object Data.SqlClient.SqlDataAdapter $command
        $dataset = New-Object Data.DataSet
        $adapter.Fill($dataSet) | Out-Null
    }
    finally
    {
        if ($PSCmdlet.ParameterSetName -eq 'ConnectionString')
        {
            $Connection.Dispose()
        }
    }

    $dataSet.Tables
}

function Invoke-BatchSQLFromFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$Script,
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection,
        [Switch]$UseBCP,
        [int]$CommandExecutionTimeout
    )

    $CommandExecuted = $false
    if ($UseBCP)
    {
        $SeparateLines = $Script -split "`n"
        if ($SeparateLines -gt 1)
        {
            $ArtifactLine = $SeparateLines[0].Trim()
            $TablePrefixLine = $SeparateLines[1].Trim()
            if ($ArtifactLine.StartsWith($script:ArtifactNameLinePrefix) -and $TablePrefixLine.StartsWith($script:TableNameLinePrefix))
            {
                $ArtifactName = $ArtifactLine.Substring($script:ArtifactNameLinePrefix.Length).Trim()
                $TableName = $TablePrefixLine.Substring($script:TableNameLinePrefix.Length).Trim()

                $CommandExecuted = $true
                $scriptExecutionResult = Invoke-SQL -Connection $Connection -SqlCommand $Script -Timeout $CommandExecutionTimeout
                $shouldExecuteBcp = ($scriptExecutionResult.Count -gt 0) -and ($scriptExecutionResult[0].Rows.Count -gt 0)
                if ($shouldExecuteBcp)
                {
                    $ArtifactsTempFolder = Join-Path (Resolve-Path .) /artifacts
                    $ArtifactFileName = $ArtifactName.Split("/") | Select-Object -Last 1
                    $LocalFileStoragePath = Join-Path $ArtifactsTempFolder $ArtifactFileName

                    if (!(Test-Path $LocalFileStoragePath))
                    {
                        Write-Verbose "Attempt to download $ArtifactName file to $LocalFileStoragePath"
                        Import-Module BitsTransfer
                        Start-BitsTransfer -Source $ArtifactName -Destination $LocalFileStoragePath
                    }

                    Write-Verbose "Starting applying BCP patch to database"
                    & bcp.exe "$($Connection.Database).$($script:DefaultSchema).$TableName" IN $LocalFileStoragePath -E -n -S -U -P
                    Write-Information "$LocalFileStoragePath was executed"
                }
            }
        }
    }

    if (!$CommandExecuted)
    {
        Invoke-SQL -Connection $Connection -SqlCommand $Script -Timeout $CommandExecutionTimeout
    }
}

function Invoke-SQLFromFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [ValidateScript({ [IO.File]::Exists((Resolve-Path $_)) })]
        [string]$FilePath,
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection,
        [Switch]$UseBCP
    )
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
        # split the file in batch commands (separated by command terminator statements) and run them
        foreach ($line in $fileText)
        {
            if ($line -match '^\s*' + $script:CommandTerminator + '\s*$')
            {
                # exec
                Invoke-BatchSQLFromFile -Script $batchScript -Connection $Connection -UseBCP:$UseBCP -CommandExecutionTimeout $commandExecutionTimeout
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
        Invoke-BatchSQLFromFile -Script $batchScript -Connection $Connection -UseBCP:$UseBCP -CommandExecutionTimeout $commandExecutionTimeout
        Write-Information "File $FilePath executed successfully"
    }
    catch [Data.SqlClient.SqlException]
    {
        $OccurredException = $_.Exception.InnerException
        $ExceptionLine = $lineBlockStart + $OccurredException.LineNumber
        throw "Error executing file $($FilePath):
        $($OccurredException.Message)
        At line $ExceptionLine, Error Number: $($OccurredException.Number), State: $($OccurredException.State), Class: $($OccurredException.Class)"
    }
}

function Invoke-MigrationsInFolder {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,
        [string]$FolderPath,
        [switch]$Recurse,
        [Switch]$UseBCP
    )

    $Connection = New-Object Data.SqlClient.SqlConnection($ConnectionString)
    try
    {
        $Connection.Open()
        Initialize-HistoryTable -Connection $Connection

        $executedFilesCount = 0
        Get-ChildItem $FolderPath -File -Filter *.sql -Recurse:$Recurse |
            Sort-Object $_.Name |
            ForEach-Object {
                $fileName = $_.Name
                $executeScript = !(Test-FileMigrated -Connection $Connection -FileName $fileName)
                if ($executeScript)
                {
                    $ExecutionTimer = [Diagnostics.Stopwatch]::StartNew()
                    Invoke-SQLFromFile -FilePath $_.FullName -Connection $Connection

                    Set-FileMigrated -Connection $Connection -FileName $fileName -ExecutionDuration $ExecutionTimer.Elapsed
                    ++$executedFilesCount
                }
                else
                {
                    Write-Information "Skipped $fileName"
                }
            }
        Write-Information "Successfully executed $executedFilesCount migration scripts."
    }
    finally
    {
        $Connection.Dispose()
    }
}

function Get-PatchHistoryTableName {
    $script:FullHistoryTableName
}

function Test-FileMigrated {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    $SearchCommand = "
If Exists (Select * From $script:FullHistoryTableName Where name = @PatchFileName)
    Select 1 As [Exists]
Else
    Select 0 As [Exists]
"
    $PatchNameParameter = New-Object Data.SqlClient.SqlParameter
    $PatchNameParameter.ParameterName = '@PatchFileName'
    $PatchNameParameter.Value = $FileName
    $fileMigrated = 0 -lt ((Invoke-SQL -Connection $Connection -SqlCommand $SearchCommand -CommandParameters $PatchNameParameter).Exists)

    $fileMigrated
}

function Set-FileMigrated {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory = $true)]
        [string]$FileName,
        [TimesPan]$ExecutionDuration
    )
    $InsertCommand = "
If Not Exists (Select * From $script:FullHistoryTableName Where name = @PatchFileName)
    Insert $script:FullHistoryTableName(name, applied_at, execution_duration)
    Values (@PatchFileName, GetUTCDate(), @ExecutionDuration)
"
    $PatchNameParameter = New-Object Data.SqlClient.SqlParameter
    $PatchNameParameter.ParameterName = '@PatchFileName'
    $PatchNameParameter.Value = $FileName
    $ExecutionDurationParameter = New-Object Data.SqlClient.SqlParameter
    $ExecutionDurationParameter.ParameterName = '@ExecutionDuration'
    $ExecutionDurationParameter.Value = if ($ExecutionDuration -eq $null) { [DBNull]::Value } else { $ExecutionDuration.Ticks }
    Invoke-SQL -Connection $Connection -SqlCommand $InsertCommand -CommandParameters $PatchNameParameter, $ExecutionDurationParameter
}

function Initialize-HistoryTable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection
    )

    $TableSchemaParameter = New-Object Data.SqlClient.SqlParameter
    $TableSchemaParameter.ParameterName = '@TableSchema'
    $TableSchemaParameter.Value = $script:DefaultSchema
    $TableNameParameter = New-Object Data.SqlClient.SqlParameter
    $TableNameParameter.ParameterName = '@TableName'
    $TableNameParameter.Value = $script:HistoryTable
    $TableExists = (Invoke-SQL -Connection $Connection `
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
    execution_duration bigint Null,
    Constraint [PK_$script:HistoryTable] Primary Key Clustered ( id ),
    Index [IX_$($script:HistoryTable)_Name] NonClustered (Name)
)"
        Invoke-SQL -Connection $Connection -SqlCommand $TableDefinitionCommand
    }
}

Set-Defaults -PatchHistoryTableName '_patch_history' -DatabaseSchema 'dbo' -DefaultCommandTimeout 60 -DataMigrationTimeout 600 -CommandTerminator 'go'
