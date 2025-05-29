# SharePoint Online PPTX Version History Cleanup Script
# Searches for PPTX files and deletes versions older than 2024

param(
    [Parameter(Mandatory=$true)]
    [string]$SiteUrl,
    [Parameter(Mandatory=$false)]
    [string]$LibraryName = "Documents",
    [datetime]$OlderThan = (Get-Date "2025-01-01"),
    [Parameter(Mandatory=$true)]
    [string]$LocalTempPath,
    [Parameter(Mandatory = $true)]
    [string]$ClientId,
    [Parameter(Mandatory = $true)]
    [string]$TenantId,
    [int]$RetryAttempts = 1,
    [int]$MaxFiles = 8000,            # Limit files per run
    [switch]$TestMode,               # Show what would be deleted without actually deleting
    [switch]$SkipProcessedFiles,     # Resume from previous run
    [int]$KeepMinVersions = 1,       # Always keep at least this many versions
    [int]$MinFileSizeMB = 10,       # Only process PPTX files larger than this
    [int]$MaxFileSizeMB = 8048,      # Only process PPTX files smaller than this
    [int]$SearchBatchSize = 500      # NEW: Search batch size to work around 500 limit
)

# Ensure required modules
$requiredModules = @("PnP.PowerShell")
foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Error "$module module is required. Install it with: Install-Module -Name $module -Scope CurrentUser"
        exit
    }
}

# Global variables for tracking
$script:FilesProcessed = 0
$script:VersionsDeleted = 0
$script:SpaceSavedBytes = 0
$script:FilesWithVersions = 0
$script:ProcessedFilesList = @()
$script:StartTime = Get-Date
$script:FilesSkippedSingleVersion = 0
$script:FilesSkippedAlreadyProcessed = 0
$script:FilesSkippedSizeLimit = 0

# Create directories and log files
if (-not (Test-Path $LocalTempPath)) {
    New-Item -ItemType Directory -Path $LocalTempPath -Force | Out-Null
}

$logPath = Join-Path $LocalTempPath "pptx_version_cleanup_log.txt"
$processedFilesPath = Join-Path $LocalTempPath "processed_pptx_files.txt"
$spaceSavingsReportPath = Join-Path $LocalTempPath "pptx_space_savings_report.txt"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
    Add-Content -Path $logPath -Value $logMessage
}

function Get-ProcessedFiles {
    if (Test-Path $processedFilesPath) {
        $files = Get-Content $processedFilesPath
        Write-Log "Loaded $($files.Count) previously processed files from exclusion list"
        return $files
    }
    Write-Log "No processed files list found at: $processedFilesPath"
    return @()
}

function Add-ProcessedFile {
    param([string]$FileName)
    Add-Content -Path $processedFilesPath -Value $FileName
}

function Get-PPTXFilesUsingPaginatedSearch {
    Write-Log "Searching for PPTX files using paginated SharePoint Search..."
    
    try {
        $minSizeBytes = $MinFileSizeMB * 1024 * 1024
        $maxSizeBytes = $MaxFileSizeMB * 1024 * 1024
        $searchQuery = "FileExtension:pptx AND Size>=$minSizeBytes AND Size<=$maxSizeBytes"
        
        Write-Log "Search Query: $searchQuery"
        Write-Log "Looking for PPTX files between $MinFileSizeMB MB and $MaxFileSizeMB MB..."
        Write-Log "Using paginated search with batch size: $SearchBatchSize"
        
        $pptxFiles = @()
        $startRow = 0
        $totalRetrieved = 0
        
        do {
            Write-Log "Searching batch starting at row $startRow..."
            
            # Execute search with pagination
            $searchResults = Submit-PnPSearchQuery -Query $searchQuery -StartRow $startRow -MaxResults $SearchBatchSize -SelectProperties "Title,Path,Size,LastModifiedTime,FileType"
            
            if ($searchResults.ResultRows.Count -eq 0) {
                Write-Log "No more results found in this batch"
                break
            }
            
            Write-Log "Retrieved $($searchResults.ResultRows.Count) results in this batch"
            
            foreach ($result in $searchResults.ResultRows) {
                try {
                    $fileInfo = [PSCustomObject]@{
                        Name = [System.IO.Path]::GetFileName($result.Path)
                        ServerRelativeUrl = $result.Path -replace "^https?://[^/]+", ""
                        SizeMB = [math]::Round([long]$result.Size / 1MB, 2)
                        TimeLastModified = [datetime]$result.LastModifiedTime
                        FullPath = $result.Path
                    }
                    
                    $pptxFiles += $fileInfo
                    $totalRetrieved++
                }
                catch {
                    Write-Log "Error processing search result: $($_.Exception.Message)" "WARN"
                }
            }
            
            $startRow += $SearchBatchSize
            
            # Check if we've reached our maximum files limit
            if ($totalRetrieved -ge $MaxFiles) {
                Write-Log "Reached maximum files limit of $MaxFiles"
                break
            }
            
            # Brief pause between batches to avoid throttling
            Start-Sleep -Seconds 2
            
        } while ($searchResults.ResultRows.Count -eq $SearchBatchSize) # Continue if we got a full batch
        
        if ($pptxFiles.Count -eq 0) {
            Write-Log "No PPTX files found matching size criteria" "WARN"
            return @()
        }
        
        # Remove duplicates (in case of overlapping results)
        $pptxFiles = $pptxFiles | Sort-Object ServerRelativeUrl -Unique
        
        # Trim to MaxFiles if needed
        if ($pptxFiles.Count -gt $MaxFiles) {
            $pptxFiles = $pptxFiles | Select-Object -First $MaxFiles
        }
        
        # Sort by size (largest first) for maximum impact
        $pptxFiles = $pptxFiles | Sort-Object SizeMB -Descending
        
        Write-Log "Found $($pptxFiles.Count) unique PPTX files totaling $([math]::Round(($pptxFiles | Measure-Object SizeMB -Sum).Sum, 2)) MB"
        
        # Show top 10 largest files for context
        if ($pptxFiles.Count -gt 0) {
            Write-Log "Top 10 largest PPTX files:"
            $pptxFiles | Select-Object -First 10 | ForEach-Object {
                Write-Log "  - $($_.Name): $($_.SizeMB) MB (Modified: $($_.TimeLastModified.ToString('yyyy-MM-dd')))"
            }
        }
        
        return $pptxFiles
    }
    catch {
        Write-Log "Paginated search failed: $($_.Exception.Message)" "ERROR"
        Write-Log "Falling back to library enumeration method..." "INFO"
        return Get-PPTXFilesUsingLibraryEnumeration
    }
}

function Get-PPTXFilesUsingLibraryEnumeration {
    Write-Log "Using library enumeration as fallback method..."
    
    try {
        $minSizeBytes = $MinFileSizeMB * 1024 * 1024
        $maxSizeBytes = $MaxFileSizeMB * 1024 * 1024
        
        Write-Log "Enumerating files in library: $LibraryName"
        Write-Log "Looking for PPTX files between $MinFileSizeMB MB and $MaxFileSizeMB MB..."
        
        # Get all files from the library
        $allFiles = Get-PnPListItem -List $LibraryName -PageSize 2000
        
        $pptxFiles = @()
        $fileCount = 0
        
        foreach ($item in $allFiles) {
            try {
                # Skip folders
                if ($item.FieldValues.FSObjType -eq 1) {
                    continue
                }
                
                $fileName = $item.FieldValues.FileLeafRef
                $fileSize = [long]$item.FieldValues.File_x0020_Size
                
                # Check if it's a PPTX file within size range
                if ($fileName -like "*.pptx" -and $fileSize -ge $minSizeBytes -and $fileSize -le $maxSizeBytes) {
                    $fileInfo = [PSCustomObject]@{
                        Name = $fileName
                        ServerRelativeUrl = $item.FieldValues.FileRef
                        SizeMB = [math]::Round($fileSize / 1MB, 2)
                        TimeLastModified = [datetime]$item.FieldValues.Modified
                        FullPath = $item.FieldValues.EncodedAbsUrl
                    }
                    
                    $pptxFiles += $fileInfo
                    $fileCount++
                    
                    if ($fileCount -ge $MaxFiles) {
                        Write-Log "Reached maximum files limit of $MaxFiles"
                        break
                    }
                }
            }
            catch {
                Write-Log "Error processing file item: $($_.Exception.Message)" "WARN"
            }
        }
        
        # Sort by size (largest first) for maximum impact
        $pptxFiles = $pptxFiles | Sort-Object SizeMB -Descending
        
        Write-Log "Found $($pptxFiles.Count) PPTX files via library enumeration totaling $([math]::Round(($pptxFiles | Measure-Object SizeMB -Sum).Sum, 2)) MB"
        
        return $pptxFiles
    }
    catch {
        Write-Log "Library enumeration also failed: $($_.Exception.Message)" "ERROR"
        return @()
    }
}

function Get-FileVersionsWithRetry {
    param(
        [string]$FileUrl,
        [int]$RetryCount = 0
    )
    
    try {
        $versions = Get-PnPFileVersion -Url $FileUrl
        Write-Log "Found $($versions.Count) versions for: $([System.IO.Path]::GetFileName($FileUrl))"
        return $versions
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Failed to get versions for $([System.IO.Path]::GetFileName($FileUrl)): $errorMsg" "ERROR"
        
        # Check for throttling
        if ($errorMsg -like "*throttle*" -or $errorMsg -like "*429*" -or $errorMsg -like "*rate limit*") {
            Write-Log "Throttling detected. Waiting 30 seconds..." "WARN"
            Start-Sleep -Seconds 30
        }
        
        if ($RetryCount -lt $RetryAttempts) {
            $waitTime = [Math]::Pow(2, $RetryCount) * 5
            Write-Log "Retrying in $waitTime seconds..." "INFO"
            Start-Sleep -Seconds $waitTime
            return Get-FileVersionsWithRetry -FileUrl $FileUrl -RetryCount ($RetryCount + 1)
        }
        
        return @()
    }
}

function Remove-OldVersionsWithRetry {
    param(
        [object]$Version,
        [string]$FileUrl,
        [string]$FileName,
        [int]$RetryCount = 0
    )
    
    try {
        $versionSizeMB = [math]::Round($Version.Size / 1MB, 2)
        
        if ($TestMode) {
            Write-Log "[TEST MODE] Would delete version $($Version.VersionLabel) of $FileName (Created: $($Version.Created.ToString('yyyy-MM-dd')), Size: $versionSizeMB MB)" "INFO"
            return @{Success = $true; SpaceSaved = $Version.Size}
        }
        
        Write-Log "Deleting version $($Version.VersionLabel) of $FileName (Created: $($Version.Created.ToString('yyyy-MM-dd')), Size: $versionSizeMB MB)"
        
        Remove-PnPFileVersion -Url $FileUrl -Identity $Version.VersionLabel -Force
        
        Write-Log "✓ Successfully deleted version $($Version.VersionLabel)" "SUCCESS"
        return @{Success = $true; SpaceSaved = $Version.Size}
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "✗ Failed to delete version $($Version.VersionLabel): $errorMsg" "ERROR"
        
        # Check for throttling
        if ($errorMsg -like "*throttle*" -or $errorMsg -like "*429*" -or $errorMsg -like "*rate limit*") {
            Write-Log "Throttling detected during deletion. Waiting 30 seconds..." "WARN"
            Start-Sleep -Seconds 30
        }
        
        if ($RetryCount -lt $RetryAttempts) {
            $waitTime = [Math]::Pow(2, $RetryCount) * 5
            Write-Log "Retrying deletion in $waitTime seconds..." "INFO"
            Start-Sleep -Seconds $waitTime
            return Remove-OldVersionsWithRetry -Version $Version -FileUrl $FileUrl -FileName $FileName -RetryCount ($RetryCount + 1)
        }
        
        return @{Success = $false; SpaceSaved = 0}
    }
}

function Process-PPTXFileVersions {
    param([object]$File)
    
    try {
        Write-Log ""
        Write-Log "=" * 60
        Write-Log "Processing: $($File.Name) ($($File.SizeMB) MB)"
        Write-Log "=" * 60
        
        # Get all versions for this file
        $versions = Get-FileVersionsWithRetry -FileUrl $File.ServerRelativeUrl
        
        if ($versions.Count -eq 0) {
            Write-Log "No versions found for: $($File.Name)"
            return @{VersionsDeleted = 0; SpaceSaved = 0; TotalVersions = 0; Skipped = $true; SkipReason = "NoVersions"}
        }
        
        # NEW CHECK: Skip files with only one version (current version only)
        if ($versions.Count -le 1) {
            Write-Log "Skipping $($File.Name) - only has $($versions.Count) version(s)" "INFO"
            $script:FilesSkippedSingleVersion++
            return @{VersionsDeleted = 0; SpaceSaved = 0; TotalVersions = $versions.Count; Skipped = $true; SkipReason = "SingleVersion"}
        }
        
        # Filter versions older than the specified date
        $oldVersions = $versions | Where-Object { 
            $_.Created -lt $OlderThan 
        }
        
        if ($oldVersions.Count -eq 0) {
            Write-Log "No versions older than $($OlderThan.ToString('yyyy-MM-dd')) found for: $($File.Name)"
            return @{VersionsDeleted = 0; SpaceSaved = 0; TotalVersions = $versions.Count; Skipped = $true; SkipReason = "NoOldVersions"}
        }
        
        # Sort all versions by creation date (newest first) and apply minimum versions rule
        $sortedVersions = $versions | Sort-Object Created -Descending
        $versionsToKeep = $sortedVersions | Select-Object -First $KeepMinVersions
        $versionsToDelete = $oldVersions | Where-Object { $_.VersionLabel -notin $versionsToKeep.VersionLabel }
        
        if ($versionsToDelete.Count -eq 0) {
            Write-Log "No versions can be deleted for $($File.Name) (keeping minimum $KeepMinVersions versions)"
            return @{VersionsDeleted = 0; SpaceSaved = 0; TotalVersions = $versions.Count; Skipped = $true; SkipReason = "MinVersionsRule"}
        }
        
        # Sort versions to delete by creation date (oldest first)
        $versionsToDelete = $versionsToDelete | Sort-Object Created
        
        Write-Log "Found $($versions.Count) total versions, will delete $($versionsToDelete.Count) old versions (keeping $KeepMinVersions most recent)"
        
        # Show what will be deleted
        Write-Log "Versions to delete:"
        foreach ($version in $versionsToDelete) {
            $versionSizeMB = [math]::Round($version.Size / 1MB, 2)
            Write-Log "  - Version $($version.VersionLabel): Created $($version.Created.ToString('yyyy-MM-dd HH:mm')) ($versionSizeMB MB)"
        }
        
        $totalSpaceSaved = 0
        $deletedCount = 0
        
        foreach ($version in $versionsToDelete) {
            $result = Remove-OldVersionsWithRetry -Version $version -FileUrl $File.ServerRelativeUrl -FileName $File.Name
            
            if ($result.Success) {
                $deletedCount++
                $totalSpaceSaved += $result.SpaceSaved
                $script:VersionsDeleted++
            }
            
            # Brief pause between deletions to avoid throttling
            Start-Sleep -Seconds 2
        }
        
        if ($deletedCount -gt 0) {
            $script:FilesWithVersions++
            $spaceSavedMB = [math]::Round($totalSpaceSaved / 1MB, 2)
            Write-Log "✓ Completed: Deleted $deletedCount versions, saved $spaceSavedMB MB" "SUCCESS"
        }
        
        return @{
            VersionsDeleted = $deletedCount
            SpaceSaved = $totalSpaceSaved
            TotalVersions = $versions.Count
            Skipped = $false
        }
    }
    catch {
        Write-Log "Error processing versions for $($File.Name): $($_.Exception.Message)" "ERROR"
        return @{VersionsDeleted = 0; SpaceSaved = 0; TotalVersions = 0; Skipped = $true; SkipReason = "Error"}
    }
}

function Add-ProcessedFileRecord {
    param(
        [string]$FileName,
        [string]$FileUrl,
        [decimal]$FileSizeMB,
        [int]$TotalVersions,
        [int]$VersionsDeleted,
        [long]$SpaceSaved,
        [bool]$Skipped = $false,
        [string]$SkipReason = ""
    )
    
    $record = [PSCustomObject]@{
        FileName = $FileName
        FileUrl = $FileUrl
        FileSizeMB = $FileSizeMB
        TotalVersions = $TotalVersions
        VersionsDeleted = $VersionsDeleted
        SpaceSavedMB = [math]::Round($SpaceSaved / 1MB, 2)
        Skipped = $Skipped
        SkipReason = $SkipReason
        ProcessedTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }
    
    $script:ProcessedFilesList += $record
    $script:SpaceSavedBytes += $SpaceSaved
}

function Generate-SpaceSavingsReport {
    Write-Log "Generating detailed space savings report..."
    
    $reportContent = @()
    $reportContent += "SharePoint PPTX Version History Cleanup - Space Savings Report"
    $reportContent += "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $reportContent += "Cleanup Date Threshold: $($OlderThan.ToString('yyyy-MM-dd'))"
    $reportContent += "Minimum Versions Kept: $KeepMinVersions"
    $reportContent += "File Size Range: $MinFileSizeMB MB - $MaxFileSizeMB MB"
    $reportContent += "Search Batch Size: $SearchBatchSize"
    $reportContent += "Test Mode: $TestMode"
    $reportContent += "=" * 80
    $reportContent += ""
    
    if ($script:ProcessedFilesList.Count -gt 0) {
        $reportContent += "PPTX FILES PROCESSED:"
        $reportContent += "-" * 50
        $reportContent += ""
        
        # Sort by space saved descending
        $sortedFiles = $script:ProcessedFilesList | Sort-Object SpaceSavedMB -Descending
        
        foreach ($file in $sortedFiles) {
            $reportContent += "File: $($file.FileName)"
            $reportContent += "  File Size: $($file.FileSizeMB) MB"
            $reportContent += "  Total Versions: $($file.TotalVersions)"
            if ($file.Skipped) {
                $reportContent += "  Status: SKIPPED ($($file.SkipReason))"
            } else {
                $reportContent += "  Versions Deleted: $($file.VersionsDeleted)"
                $reportContent += "  Space Saved: $($file.SpaceSavedMB) MB"
            }
            $reportContent += "  URL: $($file.FileUrl)"
            $reportContent += "  Processed: $($file.ProcessedTime)"
            $reportContent += ""
        }
        
        # Top space savers
        $reportContent += "TOP 10 SPACE SAVERS:"
        $reportContent += "-" * 30
        $topSavers = $sortedFiles | Where-Object { $_.SpaceSavedMB -gt 0 } | Select-Object -First 10
        foreach ($file in $topSavers) {
            $reportContent += "$($file.FileName): $($file.SpaceSavedMB) MB saved ($($file.VersionsDeleted) versions)"
        }
        $reportContent += ""
        
        # Skipped files summary
        $skippedFiles = $script:ProcessedFilesList | Where-Object { $_.Skipped }
        if ($skippedFiles.Count -gt 0) {
            $reportContent += "SKIPPED FILES SUMMARY:"
            $reportContent += "-" * 30
            $skipReasons = $skippedFiles | Group-Object SkipReason
            foreach ($reason in $skipReasons) {
                $reportContent += "$($reason.Name): $($reason.Count) files"
            }
            $reportContent += ""
        }
    }
    
    # Overall statistics
    $totalSpaceSavedMB = [math]::Round($script:SpaceSavedBytes / 1MB, 2)
    $totalSpaceSavedGB = [math]::Round($totalSpaceSavedMB / 1024, 2)
    
    $reportContent += "OVERALL STATISTICS:"
    $reportContent += "-" * 50
    $reportContent += "Total PPTX Files Processed: $($script:FilesProcessed)"
    $reportContent += "Files with Versions Cleaned: $($script:FilesWithVersions)"
    $reportContent += "Files Skipped (Single Version): $($script:FilesSkippedSingleVersion)"
    $reportContent += "Files Skipped (Already Processed): $($script:FilesSkippedAlreadyProcessed)"
    $reportContent += "Files Skipped (Size Limits): $($script:FilesSkippedSizeLimit)"
    $reportContent += "Total Versions Deleted: $($script:VersionsDeleted)"
    $reportContent += "Total Space Saved: $totalSpaceSavedMB MB ($totalSpaceSavedGB GB)"
    
    if ($script:FilesProcessed -gt 0) {
        $avgSpaceSaved = [math]::Round($totalSpaceSavedMB / $script:FilesProcessed, 2)
        $reportContent += "Average Space Saved per File: $avgSpaceSaved MB"
    }
    
    $elapsed = (Get-Date) - $script:StartTime
    $reportContent += "Processing Time: $($elapsed.ToString('hh\:mm\:ss'))"
    
    # Write to file
    $reportContent | Out-File -FilePath $spaceSavingsReportPath -Encoding UTF8
    Write-Log "Space savings report generated: $spaceSavingsReportPath" "SUCCESS"
    
    return @{
        TotalSpaceSavedMB = $totalSpaceSavedMB
        TotalSpaceSavedGB = $totalSpaceSavedGB
        VersionsDeleted = $script:VersionsDeleted
    }
}

function Show-Progress {
    param([int]$Current, [int]$Total, [string]$CurrentFile)
    
    $elapsed = (Get-Date) - $script:StartTime
    $percentComplete = [math]::Round(($Current / $Total) * 100, 1)
    
    Write-Host ""
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host "PROGRESS: $Current/$Total PPTX files ($percentComplete%)" -ForegroundColor Yellow
    Write-Host "Current: $CurrentFile" -ForegroundColor White
    Write-Host "Files with Versions Cleaned: $($script:FilesWithVersions)" -ForegroundColor Green
    Write-Host "Files Skipped (Single Version): $($script:FilesSkippedSingleVersion)" -ForegroundColor Gray
    Write-Host "Files Skipped (Already Processed): $($script:FilesSkippedAlreadyProcessed)" -ForegroundColor Gray
    Write-Host "Total Versions Deleted: $($script:VersionsDeleted)" -ForegroundColor Red
    Write-Host "Space Saved: $([math]::Round($script:SpaceSavedBytes / 1MB, 2)) MB" -ForegroundColor Cyan
    Write-Host "Elapsed Time: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Gray
    if ($Current -gt 0) {
        $eta = $elapsed.TotalSeconds * ($Total - $Current) / $Current
        $etaSpan = [TimeSpan]::FromSeconds($eta)
        Write-Host "ETA: $($etaSpan.ToString('hh\:mm\:ss'))" -ForegroundColor Yellow
    }
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host ""
}

# Main execution
Write-Log "Starting SharePoint PPTX version history cleanup..."
Write-Log "Site URL: $SiteUrl"
Write-Log "Library: $LibraryName"
Write-Log "Delete versions older than: $($OlderThan.ToString('yyyy-MM-dd'))"
Write-Log "Keep minimum versions: $KeepMinVersions"
Write-Log "File size range: $MinFileSizeMB MB - $MaxFileSizeMB MB"
Write-Log "Max Files per Run: $MaxFiles"
Write-Log "Search Batch Size: $SearchBatchSize"
Write-Log "Test Mode: $TestMode"
Write-Log "Skip files with single version: YES"

try {
    # Connect to SharePoint
    Write-Log "Connecting to SharePoint site..."
    Connect-PnPOnline -Url $SiteUrl -ClientId $ClientId -Tenant $TenantId -Interactive
    Write-Log "Successfully connected to SharePoint!"
    
    # Search for PPTX files using paginated approach
    Write-Log "Starting PPTX file discovery..."
    $pptxFiles = Get-PPTXFilesUsingPaginatedSearch
    
    if ($pptxFiles.Count -eq 0) {
        Write-Log "No PPTX files found matching criteria" "WARN"
        exit
    }
    
    # Get processed files for exclusion
    $processedFiles = Get-ProcessedFiles
    
    # Process PPTX files
    $fileIndex = 0
    foreach ($file in $pptxFiles) {
        $fileIndex++
        
        # Skip if already processed (from exclusion list)
        if ($processedFiles -contains $file.Name) {
            Write-Log "Skipping already processed file: $($file.Name)" "INFO"
            $script:FilesSkippedAlreadyProcessed++
            
            # Still record it for reporting
            Add-ProcessedFileRecord -FileName $file.Name -FileUrl $file.ServerRelativeUrl -FileSizeMB $file.SizeMB -TotalVersions 0 -VersionsDeleted 0 -SpaceSaved 0 -Skipped $true -SkipReason "AlreadyProcessed"
            continue
        }
        
        Show-Progress -Current $fileIndex -Total $pptxFiles.Count -CurrentFile $file.Name
        
        try {
            $result = Process-PPTXFileVersions -File $file
            
            # Record the processing results
            Add-ProcessedFileRecord -FileName $file.Name -FileUrl $file.ServerRelativeUrl -FileSizeMB $file.SizeMB -TotalVersions $result.TotalVersions -VersionsDeleted $result.VersionsDeleted -SpaceSaved $result.SpaceSaved -Skipped $result.Skipped -SkipReason $result.SkipReason
            
            $script:FilesProcessed++
            
            # Only add to processed files list if we actually attempted to process it (not skipped due to single version)
            if (-not $result.Skipped -or $result.SkipReason -ne "SingleVersion") {
                Add-ProcessedFile $file.Name
            }
        }
        catch {
            Write-Log "Error processing $($file.Name): $($_.Exception.Message)" "ERROR"
        }
        
        # Brief pause between files to avoid throttling
        Start-Sleep -Seconds 3
    }
    
    # Final summary with detailed report
    $totalElapsed = (Get-Date) - $script:StartTime
    
    # Generate detailed report
    $spaceStats = Generate-SpaceSavingsReport
    
    Write-Log ""
    Write-Log "========== FINAL SUMMARY =========="
    Write-Log "Total PPTX Files Processed: $($script:FilesProcessed)"
    Write-Log "Files with Versions Cleaned: $($script:FilesWithVersions)"
    Write-Log "Files Skipped (Single Version): $($script:FilesSkippedSingleVersion)"
    Write-Log "Files Skipped (Already Processed): $($script:FilesSkippedAlreadyProcessed)"
    Write-Log "Total Versions Deleted: $($spaceStats.VersionsDeleted)"
    Write-Log "Total Space Saved: $($spaceStats.TotalSpaceSavedMB) MB ($($spaceStats.TotalSpaceSavedGB) GB)"
    Write-Log "Total Processing Time: $($totalElapsed.ToString('hh\:mm\:ss'))"
    Write-Log ""
    Write-Log "Detailed report saved to: $spaceSavingsReportPath"
    Write-Log "============================================="
    
    if ($TestMode) {
        Write-Log ""
        Write-Log "*** TEST MODE WAS ENABLED - NO VERSIONS WERE ACTUALLY DELETED ***" "WARN"
        Write-Log "Run without -TestMode to perform actual cleanup" "WARN"
    }
}
catch {
    Write-Log "Critical error: $($_.Exception.Message)" "ERROR"
    Write-Log "Stack trace: $($_.ScriptStackTrace)" "ERROR"
}
finally {
    try {
        Disconnect-PnPOnline
        Write-Log "Disconnected from SharePoint"
    } catch {
        Write-Log "Error during disconnect: $($_.Exception.Message)" "ERROR"
    }
}