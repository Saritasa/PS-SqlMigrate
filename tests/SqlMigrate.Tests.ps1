Import-Module ..\SqlMigrate.psm1

$testDatabaseConnectionString = "Data Source=localhost; Integrated Security=SSPI; Initial Catalog=tempdb"
# $testDatabaseConnectionString = "Data Source=localhost; Integrated Security=SSPI; Initial Catalog=CRM.Production"
$HistoryTable = Get-PatchHistoryTableName

Describe "Initialize-HistoryTable" {
    It "should throw if invalid connection string is given" {
        { Initialize-HistoryTable "asdf" } | Should throw
    }

    Context "Successful table creation" {
        It "should create an empty history table" {
            Initialize-HistoryTable $testDatabaseConnectionString
    
            $res = Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Select Count(*) As [Count] From $HistoryTable"
            $res.Count | Should Be 0
        }
    
        It "should not fail if history table already exists" {
            Initialize-HistoryTable $testDatabaseConnectionString
            { Initialize-HistoryTable $testDatabaseConnectionString } | Should Not throw
        }
    
        AfterEach {
            Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Drop Table $HistoryTable" -ErrorAction SilentlyContinue
        }
    }
}

Describe "Invoke-SQLFromFile" {
    It "should fail on executing the script" {
        { Invoke-SQLFromFile -FilePath .\TestScripts\FailTest.sql -ConnectionString $testDatabaseConnectionString } | should throw        
    }
    Context "working with _test_table" {
        It "should successfully execute script" {
            Invoke-SQLFromFile -FilePath .\TestScripts\SimpleScript.sql -ConnectionString $testDatabaseConnectionString
            $res = Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Select Count(*) As [Count] From [_test_table]"
            $res.Count | Should Be 2
        }

        AfterEach {
            # cleanup the table
            Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Drop Table [_test_table]" -ErrorAction SilentlyContinue            
        }
    }
}

Describe "Set-FileMigrated" {
    Initialize-HistoryTable $testDatabaseConnectionString

    It "should successfully set file migration status" {
        $FileName = 'test.sql'
        Set-FileMigrated -ConnectionString $testDatabaseConnectionString -FileName $FileName

        $FileNameParameter = New-Object System.Data.SqlClient.SqlParameter
        $FileNameParameter.ParameterName = '@FileName'
        $FileNameParameter.Value = $FileName    
        $res = Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Select Count(*) As [Count] From $HistoryTable Where name = @FileName" `
            -CommandParameters $FileNameParameter
        $res.Count | Should Be 1
    }
    
    It "should not insert duplicate files" {
        $FileName = 'test.sql'
        Set-FileMigrated -ConnectionString $testDatabaseConnectionString -FileName $FileName
        Set-FileMigrated -ConnectionString $testDatabaseConnectionString -FileName $FileName

        $FileNameParameter = New-Object System.Data.SqlClient.SqlParameter
        $FileNameParameter.ParameterName = '@FileName'
        $FileNameParameter.Value = $FileName    
        $res = Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Select Count(*) As [Count] From $HistoryTable Where name = @FileName" `
            -CommandParameters $FileNameParameter
        $res.Count | Should Be 1
    }
    
    AfterEach {
        # clear the patch table
        Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Delete $HistoryTable" -ErrorAction SilentlyContinue            
    }
    
    Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Drop Table $HistoryTable" -ErrorAction SilentlyContinue
}