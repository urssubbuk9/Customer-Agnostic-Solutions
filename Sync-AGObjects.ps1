<#
.SYNOPSIS
    Synchronise Agent Jobs/Logins between Availability Group Replicas
.DESCRIPTION
    Connect to the AG listeners, get the name of the primary replica and all secondaries
.EXAMPLE
    PS C:\> .\Sync-AGObjects.ps1 -AGListenerName AG1
.INPUTS
    AGListenerName - name of the Availability Group you'd like to synchronise objects within.
.NOTES
    - The DBATools module (dbatools.io) is required to execute the underlying commands.
    - The -WhatIf parameter is included at first to test the script. Remove this once ready to sync.
#>

# Define the Availability Group Listener
param 
(
    [Parameter(Mandatory = $true)]
    [string]$AGListenerName  
)

# Initialise Variables
$ClientName = 'AG Synchronisation Tool'
$primaryInstance = $null
$secondaryInstances = @{}

try {

    # Connect to the SQL Server and return the name of the Primary AG replica and all secondaries
    $replicas = Get-DbaAgReplica -SqlInstance $AGListenerName 
    $primaryInstance = $replicas | Where-Object Role -eq 'Primary' | Select-Object -ExpandProperty name
    $secondaryInstances = $replicas | Where-Object Role -ne 'Primary' | Select-Object -ExpandProperty name

    # Create a connection to the Primary AG replica
    $primaryInstanceConnection = Connect-DbaInstance $primaryInstance -ClientName $ClientName

    # loop through each secondary replica and sync the logins and jobs
    $secondaryInstances | ForEach-Object {
        $secondaryInstanceConnection = Connect-DbaInstance $_ -ClientName $ClientName

        # Copy Agent Jobs
        Write-Output "Copying SQL Agent Jobs to $_.."
        Copy-DbaAgentJob -Source $primaryInstanceConnection -Destination $secondaryInstanceConnection -WhatIf

        # Copy Logins
        Write-Output "Copying SQL Logins to $_.."
        Copy-DbaLogin -Source $primaryInstanceConnection -Destination $secondaryInstanceConnection -ObjectLevel -ExcludeSystemLogins -WhatIf
    }
}

catch {
    
    # Display error messages if returned
    $ErrorMessage = $_.Exception.Message
    Write-Error "Error while synchronising jobs/logins for Availability Group '$($AGListenerName): $ErrorMessage'"

    # Ensure SQL Agent job fails
    [System.Environment]::Exit(1)
}






