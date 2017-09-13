Import-Module ..\SqlMigrate.psm1

$testDatabaseConnectionString = "Data Source=localhost; Integrated Security=SSPI; Initial Catalog=tempdb"
# $testDatabaseConnectionString = "Data Source=localhost; Integrated Security=SSPI; Initial Catalog=CRM.Production"
$HistoryTable = Get-PatchHistoryTableName

Describe "Initialize-HistoryTable" {
    Context "Successful table creation" {
        $Connection = New-Object Data.SqlClient.SqlConnection($testDatabaseConnectionString)
        $Connection.Open()
        It "should create an empty history table" {
            Initialize-HistoryTable $Connection
    
            $res = Invoke-SQL -Connection $Connection -sqlCommand "Select Count(*) As [Count] From $HistoryTable"
            $res.Count | Should Be 0
        }
    
        It "should not fail if history table already exists" {
            Initialize-HistoryTable $Connection
            { Initialize-HistoryTable $Connection } | Should Not throw
        }
    
        AfterEach {
            Invoke-SQL -Connection $Connection -sqlCommand "Drop Table $HistoryTable" -ErrorAction SilentlyContinue
        }
        $Connection.Close()        
    }
}

Describe "Invoke-SQLFromFile" {
    $Connection = New-Object Data.SqlClient.SqlConnection($testDatabaseConnectionString)
    $Connection.Open()
    It "should fail on executing the script" {
        { Invoke-SQLFromFile -FilePath .\TestScripts\FailTest.sql -Connection $Connection } | should throw        
    }
    Context "working with _test_table" {
        It "should successfully execute script" {
            Invoke-SQLFromFile -FilePath .\TestScripts\SimpleScript.sql -Connection $Connection
            $res = Invoke-SQL -Connection $Connection -sqlCommand "Select Count(*) As [Count] From [_test_table]"
            $res.Count | Should Be 2
        }

        AfterEach {
            # cleanup the table
            Invoke-SQL -Connection $Connection -sqlCommand "Drop Table [_test_table]" -ErrorAction SilentlyContinue            
        }
    }
    $Connection.Close()        
}

Describe "Set-FileMigrated" {
    $Connection = New-Object Data.SqlClient.SqlConnection($testDatabaseConnectionString)
    $Connection.Open()
    Initialize-HistoryTable $Connection

    It "should successfully set file migration status" {
        $FileName = 'test.sql'
        Set-FileMigrated -Connection $Connection -FileName $FileName

        $FileNameParameter = New-Object System.Data.SqlClient.SqlParameter
        $FileNameParameter.ParameterName = '@FileName'
        $FileNameParameter.Value = $FileName    
        $res = Invoke-SQL -Connection $Connection -sqlCommand "Select Count(*) As [Count] From $HistoryTable Where name = @FileName" `
            -CommandParameters $FileNameParameter
        $res.Count | Should Be 1
    }
    
    It "should not insert duplicate files" {
        $FileName = 'test.sql'
        Set-FileMigrated -Connection $Connection -FileName $FileName
        Set-FileMigrated -Connection $Connection -FileName $FileName

        $FileNameParameter = New-Object System.Data.SqlClient.SqlParameter
        $FileNameParameter.ParameterName = '@FileName'
        $FileNameParameter.Value = $FileName    
        $res = Invoke-SQL -Connection $Connection -sqlCommand "Select Count(*) As [Count] From $HistoryTable Where name = @FileName" `
            -CommandParameters $FileNameParameter
        $res.Count | Should Be 1
    }
    
    AfterEach {
        # clear the patch table
        Invoke-SQL -Connection $Connection -sqlCommand "Delete $HistoryTable" -ErrorAction SilentlyContinue            
    }
    
    Invoke-SQL -Connection $Connection -sqlCommand "Drop Table $HistoryTable" -ErrorAction SilentlyContinue
    $Connection.Close()        
}

Describe "Test-FileMigrated" {
    $Connection = New-Object Data.SqlClient.SqlConnection($testDatabaseConnectionString)
    $Connection.Open()
    Initialize-HistoryTable $Connection
    
    It "should succsessfuly detect file was not migrated" {
        $FileName = 'test.sql'
        (Test-FileMigrated -Connection $Connection -FileName $FileName) | Should Be $false
    }

    It "should detect the file was migrated" {
        $FileName = 'test.sql'
        Set-FileMigrated -Connection $Connection -FileName $FileName
        (Test-FileMigrated -Connection $Connection -FileName $FileName) | Should Be $true
    }
    
    It "should be case insensitive" {
        $FileName = 'test.sql'
        Set-FileMigrated -Connection $Connection -FileName $FileName
        (Test-FileMigrated -Connection $Connection -FileName $FileName.ToUpper()) | Should Be $true
    }
    
    AfterEach {
        # clear the patch table
        Invoke-SQL -Connection $Connection -sqlCommand "Delete $HistoryTable" -ErrorAction SilentlyContinue            
    }
    
    Invoke-SQL -Connection $Connection -sqlCommand "Drop Table $HistoryTable" -ErrorAction SilentlyContinue
    $Connection.Close()        
}