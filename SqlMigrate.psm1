[string]$script:HistoryTable = $null
[string]$script:DefaultSchema = $null
[string]$script:FullHistoryTableName = $null
[string]$script:CommandTerminator = $null
[int]$script:DefaultCommandTimeout = $null
[int]$script:DataMigrationTimeout = $null

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
        if ($Connection.State -ne [Data.ConnectionState]::Open)
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

function Invoke-SQLFromFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [ValidateScript({ [IO.File]::Exists((Resolve-Path $_)) })]
        [string]$FilePath,
        [Parameter(Mandatory = $true)]
        [Data.SqlClient.SqlConnection]$Connection
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
                Invoke-SQL -Connection $Connection -SqlCommand $batchScript -Timeout $commandExecutionTimeout
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
        Invoke-SQL -Connection $Connection -SqlCommand $batchScript -Timeout $commandExecutionTimeout
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
        [switch]$Recurse
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
                    Invoke-SQLFromFile -FilePath $_.FullName -Connection $Connection
                    Set-FileMigrated -Connection $Connection -FileName $fileName
                    ++$executedFilesCount
                }
            }
        Write-Information "Successfully executed $executedFilesCount migration scripts."        
    }
    finally
    {
        if ($Connection.State -eq "Open")
        {
            $Connection.Dispose()
        }
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
        [string]$FileName
    )
    $InsertCommand = "
If Not Exists (Select * From $script:FullHistoryTableName Where name = @PatchFileName)
    Insert $script:FullHistoryTableName(name, applied_at)
    Values (@PatchFileName, GetUTCDate())
"
    $PatchNameParameter = New-Object Data.SqlClient.SqlParameter
    $PatchNameParameter.ParameterName = '@PatchFileName'
    $PatchNameParameter.Value = $FileName
    Invoke-SQL -Connection $Connection -SqlCommand $InsertCommand -CommandParameters $PatchNameParameter
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
    Constraint [PK_$script:HistoryTable] Primary Key Clustered ( id ),
    Index [IX_$($script:HistoryTable)_Name] NonClustered (Name)
)"
        Invoke-SQL -Connection $Connection -SqlCommand $TableDefinitionCommand
    }
}

Set-Defaults -PatchHistoryTableName '_patch_history' -DatabaseSchema 'dbo' -DefaultCommandTimeout 60 -DataMigrationTimeout 600 -CommandTerminator 'go'
