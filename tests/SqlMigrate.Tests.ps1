Import-Module ..\SqlMigrate.psm1

$testDatabaseConnectionString = "Data Source=localhost; Integrated Security=SSPI; Initial Catalog=tempdb"
$HistoryTable = Get-PatchHistoryTableName

Describe "Initialize-HistoryTable" {
    It "should throw if invalid connection string is given" {
        { Initialize-HistoryTable "asdf" } | Should throw
    }
    
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
        try {
            Invoke-SQL -ConnectionString $testDatabaseConnectionString -sqlCommand "Drop Table $HistoryTable"            
        }
        catch {            
        }
    }
}
