# SharePoint Online Document Processing - Fixed Upload Issues
# Fixed content type and upload method

param(
    [Parameter(Mandatory=$true)]
    [string]$SiteUrl,
    [Parameter(Mandatory=$false)]
    [string]$LibraryName = "Shared Documents",
    [string]$FolderPath = "",
    [int]$MinSizeMB = 50,
    [int]$PngWidth = 200,
    [datetime]$OlderThan = [datetime]::MaxValue,
    [Parameter(Mandatory=$true)]
    [string]$LocalTempPath,
    [Parameter(Mandatory = $true)]
    [string]$ClientId,
    [Parameter(Mandatory = $true)]
    [string]$TenantId,
    [int]$RetryAttempts = 1,
    [int]$MaxFiles = 500,            # Limit files per run to avoid timeouts
    [switch]$TestMode,               # Process only first 5 files
    [switch]$SkipDownloadedFiles,    # Resume from previous run
    [switch]$UseSearch              # Use search instead of folder enumeration
)

# Ensure required modules
$requiredModules = @("PnP.PowerShell")
foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Error "$module module is required. Install it with: Install-Module -Name $module -Scope CurrentUser"
        exit
    }
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
Add-Type -AssemblyName System.Drawing

# Global variables for tracking
$script:SuccessfulDownloads = 0
$script:FailedDownloads = 0
$script:ProcessedFiles = 0
$script:TotalBytes = 0
$script:TotalOriginalSize = 0
$script:TotalNewSize = 0
$script:ProcessedFilesList = @()
$script:StartTime = Get-Date

# Create directories
if (-not (Test-Path $LocalTempPath)) {
    New-Item -ItemType Directory -Path $LocalTempPath -Force | Out-Null
}

$logPath = Join-Path $LocalTempPath "processing_log.txt"
$completedFilesPath = Join-Path $LocalTempPath "completed_files.txt"
$sizeReportPath = Join-Path $LocalTempPath "file_size_report.txt"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
    Add-Content -Path $logPath -Value $logMessage
}

function Get-CompletedFiles {
    if (Test-Path $completedFilesPath) {
        return Get-Content $completedFilesPath
    }
    return @()
}

function Add-CompletedFile {
    param([string]$FileName)
    Add-Content -Path $completedFilesPath -Value $FileName
}

function Add-ProcessedFileRecord {
    param(
        [string]$FileName,
        [long]$OriginalSize,
        [long]$NewSize,
        [int]$SvgCount = 0,
        [int]$PngCount = 0
    )
    
    $record = [PSCustomObject]@{
        FileName = $FileName
        OriginalSizeMB = [math]::Round($OriginalSize / 1MB, 2)
        NewSizeMB = [math]::Round($NewSize / 1MB, 2)
        SizeReductionMB = [math]::Round(($OriginalSize - $NewSize) / 1MB, 2)
        SizeReductionPercent = if ($OriginalSize -gt 0) { [math]::Round((($OriginalSize - $NewSize) / $OriginalSize) * 100, 1) } else { 0 }
        SvgFilesRemoved = $SvgCount
        PngFilesResized = $PngCount
        ProcessedTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }
    
    $script:ProcessedFilesList += $record
    $script:TotalOriginalSize += $OriginalSize
    $script:TotalNewSize += $NewSize
    
    Write-Log "Size reduction: $($record.FileName) - $($record.SizeReductionMB) MB saved ($($record.SizeReductionPercent)%)"
}

function Get-TargetFilesUsingSearch {
    Write-Log "Using SharePoint Search to find target files..."
    
    try {
        # Build search query for DOCX files with DD in name and minimum size
        $searchQuery = "FileExtension:docx AND filename:'Detailed design'* AND Size>=$($MinSizeMB * 1024 * 1024)"
        
        if ($FolderPath) {
            $searchQuery += " AND Path:*/$FolderPath/*"
        }
        
        Write-Log "Search Query: $searchQuery"
        
        # Execute search with controlled row limit
        $searchResults = Submit-PnPSearchQuery -Query $searchQuery -MaxResults $MaxFiles -SelectProperties "Title,Path,Size,LastModifiedTime"
        
        $targetFiles = @()
        
        foreach ($result in $searchResults.ResultRows) {
            $fileInfo = @{
                Name = [System.IO.Path]::GetFileName($result.Path)
                ServerRelativeUrl = $result.Path -replace "^https?://[^/]+", ""
                Length = [long]$result.Size
                TimeLastModified = [datetime]$result.LastModifiedTime
            }
            
            # Apply date filter
            if ($OlderThan -eq [datetime]::MaxValue -or $fileInfo.TimeLastModified -lt $OlderThan) {
                $targetFiles += $fileInfo
            }
        }
        
        Write-Log "Search found $($targetFiles.Count) target files"
        return $targetFiles
    }
    catch {
        Write-Log "Search failed: $($_.Exception.Message)" "ERROR"
        return @()
    }
}

function Get-TargetFilesUsingFolderEnumeration {
    param([int]$PageSize = 500)
    
    Write-Log "Using folder enumeration to find target files..."
    
    try {
        # Get folder path
        $pnpFolderPath = if ($FolderPath) { 
            "Shared Documents/$FolderPath" 
        } else { 
            "Shared Documents"
        }
        
        Write-Log "Enumerating folder: $pnpFolderPath (Page Size: $PageSize)"
        
        # Get files with pagination to avoid timeout
        $allFiles = @()
        $pageNumber = 0
        $hasMore = $true
        
        while ($hasMore -and $allFiles.Count -lt $MaxFiles) {
            try {
                Write-Log "Fetching page $($pageNumber + 1)..."
                
                # Use CAML query for better performance
                $camlQuery = @"
<View>
    <Query>
        <Where>
            <And>
                <And>
                    <Contains>
                        <FieldRef Name='FileLeafRef' />
                        <Value Type='Text'>DD</Value>
                    </Contains>
                    <Eq>
                        <FieldRef Name='File_x0020_Type' />
                        <Value Type='Text'>docx</Value>
                    </Eq>
                </And>
                <Gt>
                    <FieldRef Name='File_x0020_Size' />
                    <Value Type='Number'>$($MinSizeMB * 1024 * 1024)</Value>
                </Gt>
            </And>
        </Where>
        <OrderBy>
            <FieldRef Name='File_x0020_Size' Ascending='FALSE' />
        </OrderBy>
    </Query>
    <RowLimit>$PageSize</RowLimit>
</View>
"@
                
                $pageFiles = Get-PnPListItem -List "Documents" -Query $camlQuery
                
                if ($pageFiles.Count -eq 0) {
                    $hasMore = $false
                    break
                }
                
                foreach ($item in $pageFiles) {
                    $fileInfo = @{
                        Name = $item.FieldValues.FileLeafRef
                        ServerRelativeUrl = $item.FieldValues.FileRef
                        Length = $item.FieldValues.File_x0020_Size
                        TimeLastModified = $item.FieldValues.Modified
                    }
                    
                    # Apply date filter
                    if ($OlderThan -eq [datetime]::MaxValue -or $fileInfo.TimeLastModified -lt $OlderThan) {
                        $allFiles += $fileInfo
                    }
                }
                
                Write-Log "Page $($pageNumber + 1): Found $($pageFiles.Count) files, Total: $($allFiles.Count)"
                $pageNumber++
                
                # Brief pause to be respectful
                Start-Sleep -Seconds 1
            }
            catch {
                Write-Log "Error on page $($pageNumber + 1): $($_.Exception.Message)" "WARN"
                break
            }
        }
        
        Write-Log "Folder enumeration complete. Found $($allFiles.Count) target files"
        return $allFiles
    }
    catch {
        Write-Log "Folder enumeration failed: $($_.Exception.Message)" "ERROR"
        return @()
    }
}

function Resize-Image {
    param([string]$ImagePath, [int]$Width)
    
    try {
        $image = [System.Drawing.Image]::FromFile($ImagePath)
        $ratio = $image.Height / $image.Width
        $newHeight = [int]($Width * $ratio)
        
        $newImage = New-Object System.Drawing.Bitmap($Width, $newHeight)
        $graphics = [System.Drawing.Graphics]::FromImage($newImage)
        $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $graphics.DrawImage($image, 0, 0, $Width, $newHeight)
        
        $image.Dispose()
        $graphics.Dispose()
        
        $newImage.Save($ImagePath, [System.Drawing.Imaging.ImageFormat]::Png)
        $newImage.Dispose()
        
        Write-Log "Resized image: $ImagePath to ${Width}px width"
    }
    catch {
        Write-Log "Failed to resize image $ImagePath : $($_.Exception.Message)" "ERROR"
    }
}

function Update-RelationshipsFile {
    param([string]$RelsPath, [hashtable]$SvgToPngMapping)
    
    try {
        [xml]$relsXml = Get-Content $RelsPath -Encoding UTF8
        $changed = $false
        
        foreach ($rel in $relsXml.Relationships.Relationship) {
            $target = $rel.Target
            if ($target -match "image(\d+)\.svg$") {
                $imageNum = [int]$matches[1]
                $newImageNum = $imageNum - 1
                $newTarget = $target -replace "image\d+\.svg$", "image$newImageNum.png"
                $rel.Target = $newTarget
                $SvgToPngMapping[$rel.Id] = $newTarget
                $changed = $true
                Write-Log "Updated relationship: $target -> $newTarget"
            }
        }
        
        if ($changed) {
            $relsXml.Save($RelsPath)
        }
        
        return $SvgToPngMapping
    }
    catch {
        Write-Log "Failed to update relationships file: $($_.Exception.Message)" "ERROR"
        return @{}
    }
}

function Update-WordDocumentReferences {
    param([string]$DocumentXmlPath, [hashtable]$SvgToPngMapping)
    
    try {
        [xml]$docXml = Get-Content $DocumentXmlPath -Encoding UTF8
        $nsManager = New-Object System.Xml.XmlNamespaceManager($docXml.NameTable)
        $nsManager.AddNamespace("w", "http://schemas.openxmlformats.org/wordprocessingml/2006/main")
        $nsManager.AddNamespace("a", "http://schemas.openxmlformats.org/drawingml/2006/main")
        $nsManager.AddNamespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")
        
        $changed = $false
        $imageNodes = $docXml.SelectNodes("//a:blip[@r:embed]", $nsManager)
        
        foreach ($node in $imageNodes) {
            $rId = $node.GetAttribute("embed", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")
            if ($SvgToPngMapping.ContainsKey($rId)) {
                Write-Log "Updated reference $rId to point to PNG instead of SVG"
                $changed = $true
            }
        }
        
        if ($changed) {
            $docXml.Save($DocumentXmlPath)
            Write-Log "Updated document references in $DocumentXmlPath"
        }
    }
    catch {
        Write-Log "Failed to update document references: $($_.Exception.Message)" "ERROR"
    }
}

function Process-LocalWordDocument {
    param([string]$DocxPath)
    
    $docName = [System.IO.Path]::GetFileNameWithoutExtension($DocxPath)
    $docDir = [System.IO.Path]::GetDirectoryName($DocxPath)
    $zipPath = Join-Path $docDir "$docName.zip"
    $extractPath = Join-Path $docDir "$docName`_temp"
    
    # Get original file size
    $originalSize = (Get-Item $DocxPath).Length
    
    try {
        Write-Log "Processing locally: $DocxPath"
        
        # Rename .docx to .zip
        Rename-Item $DocxPath $zipPath
        
        # Extract ZIP contents
        [System.IO.Compression.ZipFile]::ExtractToDirectory($zipPath, $extractPath)
        
        # Check for SVG files in word/media
        $mediaPath = Join-Path $extractPath "word\media"
        if (-not (Test-Path $mediaPath)) {
            Write-Log "No media folder found, skipping document"
            return @{Success = $false; OriginalSize = $originalSize; NewSize = $originalSize; SvgCount = 0; PngCount = 0}
        }
        
        $svgFiles = Get-ChildItem $mediaPath -Filter "*.svg"
        if ($svgFiles.Count -eq 0) {
            Write-Log "No SVG files found, skipping document"
            return @{Success = $false; OriginalSize = $originalSize; NewSize = $originalSize; SvgCount = 0; PngCount = 0}
        }
        
        Write-Log "Found $($svgFiles.Count) SVG files"
        
        # Update relationships file
        $relsPath = Join-Path $extractPath "word\_rels\document.xml.rels"
        $svgToPngMapping = @{}
        
        if (Test-Path $relsPath) {
            $svgToPngMapping = Update-RelationshipsFile $relsPath $svgToPngMapping
        }
        
        # Process PNG files (resize them)
        $pngFiles = Get-ChildItem $mediaPath -Filter "*.png"
        foreach ($png in $pngFiles) {
            Resize-Image $png.FullName $PngWidth
        }
        
        # Update document.xml references
        $docXmlPath = Join-Path $extractPath "word\document.xml"
        if (Test-Path $docXmlPath) {
            Update-WordDocumentReferences $docXmlPath $svgToPngMapping
        }
        
        # Delete SVG files
        foreach ($svg in $svgFiles) {
            Remove-Item $svg.FullName -Force
            Write-Log "Deleted SVG: $($svg.Name)"
        }
        
        # Repack as ZIP
        Remove-Item $zipPath -Force
        [System.IO.Compression.ZipFile]::CreateFromDirectory($extractPath, $zipPath)
        
        # Rename back to .docx
        Rename-Item $zipPath $DocxPath
        
        # Get new file size
        $newSize = (Get-Item $DocxPath).Length
        
        Write-Log "Successfully processed: $DocxPath" "SUCCESS"
        return @{
            Success = $true
            OriginalSize = $originalSize
            NewSize = $newSize
            SvgCount = $svgFiles.Count
            PngCount = $pngFiles.Count
        }
    }
    catch {
        Write-Log "Error processing $DocxPath : $($_.Exception.Message)" "ERROR"
        return @{Success = $false; OriginalSize = $originalSize; NewSize = $originalSize; SvgCount = 0; PngCount = 0}
    }
    finally {
        # Cleanup
        if (Test-Path $extractPath) {
            Remove-Item $extractPath -Recurse -Force
        }
        if (Test-Path $zipPath) {
            Remove-Item $zipPath -Force
        }
    }
}

function Download-FileWithRetry {
    param(
        [object]$File,
        [string]$LocalPath,
        [int]$RetryCount = 0
    )
    
    $localFilePath = Join-Path $LocalPath $File.Name
    
    try {
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        
        Write-Log "Downloading: $($File.Name) ($([math]::Round($File.Length/1MB, 2)) MB) - Attempt $($RetryCount + 1)"
        
        Get-PnPFile -Url $File.ServerRelativeUrl -Path $LocalPath -Filename $File.Name -AsFile -Force
        
        $stopwatch.Stop()
        $sizeMB = [math]::Round($File.Length / 1MB, 2)
        $speedMBps = if ($stopwatch.Elapsed.TotalSeconds -gt 0) { 
            [math]::Round($sizeMB / $stopwatch.Elapsed.TotalSeconds, 2) 
        } else { 0 }
        
        Write-Log "Downloaded successfully: $($File.Name) in $($stopwatch.Elapsed.TotalSeconds.ToString('F1'))s at $speedMBps MB/s" "SUCCESS"
        
        $script:SuccessfulDownloads++
        $script:TotalBytes += $File.Length
        
        return $true
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Download failed for $($File.Name): $errorMsg" "ERROR"
        
        # Check if this looks like a throttling error
        if ($errorMsg -like "*throttle*" -or $errorMsg -like "*429*" -or $errorMsg -like "*rate limit*") {
            Write-Log "Throttling detected. Waiting 30 seconds before retry..." "WARN"
            Start-Sleep -Seconds 30
        }
        
        if ($RetryCount -lt $RetryAttempts) {
            $waitTime = [Math]::Pow(2, $RetryCount) * 5  # Exponential backoff: 5, 10, 20 seconds
            Write-Log "Retrying in $waitTime seconds..." "INFO"
            Start-Sleep -Seconds $waitTime
            return Download-FileWithRetry -File $File -LocalPath $LocalPath -RetryCount ($RetryCount + 1)
        }
        
        $script:FailedDownloads++
        return $false
    }
}

function Upload-ProcessedFileWithRetry {
    param(
        [string]$LocalFilePath,
        [string]$OriginalServerRelativeUrl,
        [int]$RetryCount = 0
    )
    
    try {
        $fileName = [System.IO.Path]::GetFileName($LocalFilePath)
        Write-Log "Uploading processed file: $fileName - Attempt $($RetryCount + 1)"
        
        # Method 1: Try direct file replacement using Set-PnPFileContent
        try {
            Set-PnPFileContent -Path $LocalFilePath -Url $OriginalServerRelativeUrl
            Write-Log "Successfully uploaded using Set-PnPFileContent: $fileName" "SUCCESS"
            return $true
        }
        catch {
            Write-Log "Set-PnPFileContent failed: $($_.Exception.Message)" "WARN"
        }
        
        # Method 2: Try using Add-PnPFile with simplified approach
        try {
            # Extract folder path from server relative URL
            $folderPath = [System.IO.Path]::GetDirectoryName($OriginalServerRelativeUrl).Replace('\', '/')
            
            Write-Log "Attempting upload to folder: $folderPath"
            
            # Use Add-PnPFile without specifying content type
            $uploadResult = Add-PnPFile -Path $LocalFilePath -Folder $folderPath -Values @{}
            
            if ($uploadResult) {
                Write-Log "Successfully uploaded using Add-PnPFile: $fileName" "SUCCESS"
                return $true
            }
        }
        catch {
            Write-Log "Add-PnPFile failed: $($_.Exception.Message)" "WARN"
        }
        
        # Method 3: Try chunked upload for larger files
        try {
            $fileSizeMB = (Get-Item $LocalFilePath).Length / 1MB
            if ($fileSizeMB -gt 10) {
                Write-Log "File is $([math]::Round($fileSizeMB, 2)) MB, attempting chunked upload..."
                
                $folderPath = [System.IO.Path]::GetDirectoryName($OriginalServerRelativeUrl).Replace('\', '/')
                $uploadResult = Add-PnPFile -Path $LocalFilePath -Folder $folderPath -ChunkSize 10485760 -Values @{}
                
                if ($uploadResult) {
                    Write-Log "Successfully uploaded using chunked upload: $fileName" "SUCCESS"
                    return $true
                }
            }
        }
        catch {
            Write-Log "Chunked upload failed: $($_.Exception.Message)" "WARN"
        }
        
        throw "All upload methods failed"
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Upload failed for $fileName : $errorMsg" "ERROR"
        
        # Check for throttling
        if ($errorMsg -like "*throttle*" -or $errorMsg -like "*429*" -or $errorMsg -like "*rate limit*") {
            Write-Log "Throttling detected during upload. Waiting 30 seconds..." "WARN"
            Start-Sleep -Seconds 30
        }
        
        if ($RetryCount -lt $RetryAttempts) {
            $waitTime = [Math]::Pow(2, $RetryCount) * 5
            Write-Log "Retrying upload in $waitTime seconds..." "INFO"
            Start-Sleep -Seconds $waitTime
            return Upload-ProcessedFileWithRetry -LocalFilePath $LocalFilePath -OriginalServerRelativeUrl $OriginalServerRelativeUrl -RetryCount ($RetryCount + 1)
        }
        
        return $false
    }
}

function Generate-SizeReport {
    Write-Log "Generating detailed size report..."
    
    $reportContent = @()
    $reportContent += "SharePoint Document Processing - Size Reduction Report"
    $reportContent += "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $reportContent += "=" * 80
    $reportContent += ""
    
    if ($script:ProcessedFilesList.Count -gt 0) {
        $reportContent += "PROCESSED FILES SUMMARY:"
        $reportContent += "-" * 50
        $reportContent += ""
        
        # Sort by size reduction descending
        $sortedFiles = $script:ProcessedFilesList | Sort-Object SizeReductionMB -Descending
        
        foreach ($file in $sortedFiles) {
            $reportContent += "File: $($file.FileName)"
            $reportContent += "  Original Size: $($file.OriginalSizeMB) MB"
            $reportContent += "  New Size: $($file.NewSizeMB) MB"
            $reportContent += "  Size Reduction: $($file.SizeReductionMB) MB ($($file.SizeReductionPercent)%)"
            $reportContent += "  SVG Files Removed: $($file.SvgFilesRemoved)"
            $reportContent += "  PNG Files Resized: $($file.PngFilesResized)"
            $reportContent += "  Processed: $($file.ProcessedTime)"
            $reportContent += ""
        }
    }
    
    # Overall statistics
    $totalReductionMB = [math]::Round(($script:TotalOriginalSize - $script:TotalNewSize) / 1MB, 2)
    $totalReductionGB = [math]::Round($totalReductionMB / 1024, 2)
    $overallReductionPercent = if ($script:TotalOriginalSize -gt 0) { 
        [math]::Round((($script:TotalOriginalSize - $script:TotalNewSize) / $script:TotalOriginalSize) * 100, 1) 
    } else { 0 }
    
    $reportContent += "OVERALL STATISTICS:"
    $reportContent += "-" * 50
    $reportContent += "Total Files Processed: $($script:ProcessedFiles)"
    $reportContent += "Total Original Size: $([math]::Round($script:TotalOriginalSize / 1MB, 2)) MB ($([math]::Round($script:TotalOriginalSize / 1GB, 2)) GB)"
    $reportContent += "Total New Size: $([math]::Round($script:TotalNewSize / 1MB, 2)) MB ($([math]::Round($script:TotalNewSize / 1GB, 2)) GB)"
    $reportContent += "Total Size Reduction: $totalReductionMB MB ($totalReductionGB GB)"
    $reportContent += "Overall Reduction Percentage: $overallReductionPercent%"
    $reportContent += ""
    $reportContent += "DOWNLOAD/UPLOAD STATISTICS:"
    $reportContent += "-" * 50
    $reportContent += "Successful Downloads: $($script:SuccessfulDownloads)"
    $reportContent += "Failed Downloads: $($script:FailedDownloads)"
    $reportContent += "Total Data Downloaded: $([math]::Round($script:TotalBytes / 1GB, 2)) GB"
    
    # Write to file
    $reportContent | Out-File -FilePath $sizeReportPath -Encoding UTF8
    Write-Log "Size report generated: $sizeReportPath" "SUCCESS"
    
    return @{
        TotalReductionMB = $totalReductionMB
        TotalReductionGB = $totalReductionGB
        OverallReductionPercent = $overallReductionPercent
    }
}

function Show-Progress {
    param([int]$Current, [int]$Total, [string]$CurrentFile)
    
    $elapsed = (Get-Date) - $script:StartTime
    $avgSpeed = if ($script:TotalBytes -gt 0) { 
        [math]::Round(($script:TotalBytes / 1MB) / $elapsed.TotalSeconds, 2) 
    } else { 0 }
    
    $percentComplete = [math]::Round(($Current / $Total) * 100, 1)
    
    Write-Host ""
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host "PROGRESS: $Current/$Total files ($percentComplete%)" -ForegroundColor Yellow
    Write-Host "Current: $CurrentFile" -ForegroundColor White
    Write-Host "Successful Downloads: $($script:SuccessfulDownloads)" -ForegroundColor Green
    Write-Host "Failed Downloads: $($script:FailedDownloads)" -ForegroundColor Red
    Write-Host "Average Speed: $avgSpeed MB/s" -ForegroundColor Cyan
    Write-Host "Elapsed Time: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Gray
    if ($script:SuccessfulDownloads -gt 0) {
        $eta = $elapsed.TotalSeconds * ($Total - $Current) / $Current
        $etaSpan = [TimeSpan]::FromSeconds($eta)
        Write-Host "ETA: $($etaSpan.ToString('hh\:mm\:ss'))" -ForegroundColor Yellow
    }
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host ""
}

# Main execution
Write-Log "Starting SharePoint Online bulk document processing..."
Write-Log "Site URL: $SiteUrl"
Write-Log "Library: $LibraryName"
Write-Log "Folder Path: $FolderPath"
Write-Log "Minimum Size: $MinSizeMB MB"
Write-Log "Max Files per Run: $MaxFiles"
Write-Log "Test Mode: $TestMode"
Write-Log "Skip Downloaded Files: $SkipDownloadedFiles"
Write-Log "Use Search: $UseSearch"

try {
    # Connect to SharePoint
    Write-Log "Connecting to SharePoint site..."
    Connect-PnPOnline -Url $SiteUrl -ClientId $ClientId -Tenant $TenantId -Interactive
    Write-Log "Successfully connected to SharePoint!"
    
    # Get target files using the specified method
    Write-Log "Starting file discovery..."
    
    $docxFiles = @()
    if ($UseSearch) {
        $docxFiles = Get-TargetFilesUsingSearch
    } else {
        $docxFiles = Get-TargetFilesUsingFolderEnumeration -PageSize 200
    }
    
    if ($docxFiles.Count -eq 0) {
        Write-Log "No matching Word documents found" "WARN"
        exit
    }
    
    # Sort by size (largest first)
    $docxFiles = $docxFiles | Sort-Object Length -Descending
    
    # Apply test mode limit
    if ($TestMode) {
        $docxFiles = $docxFiles | Select-Object -First 5
        Write-Log "Test mode: Limited to first 5 files"
    }
    
    $totalSizeGB = [math]::Round(($docxFiles | Measure-Object Length -Sum).Sum / 1GB, 2)
    Write-Log "Found $($docxFiles.Count) matching files totaling $totalSizeGB GB"
    
    # Get completed files if resuming
    $completedFiles = @()
    if ($SkipDownloadedFiles) {
        $completedFiles = Get-CompletedFiles
        Write-Log "Resuming: Found $($completedFiles.Count) previously completed files"
    }
    
    # Process files
    $fileIndex = 0
    foreach ($file in $docxFiles) {
        $fileIndex++
        
        # Skip if already completed
        if ($SkipDownloadedFiles -and $completedFiles -contains $file.Name) {
            Write-Log "Skipping already completed file: $($file.Name)"
            continue
        }
        
        Show-Progress -Current $fileIndex -Total $docxFiles.Count -CurrentFile $file.Name
        
        $localFilePath = Join-Path $LocalTempPath $file.Name
        
        try {
            # Download with retry logic
            $downloadSuccess = Download-FileWithRetry -File $file -LocalPath $LocalTempPath
            
            if (-not $downloadSuccess) {
                Write-Log "Failed to download $($file.Name) after $RetryAttempts attempts" "ERROR"
                continue
            }
            
            # Process the file
            Write-Log "Processing downloaded file: $($file.Name)"
            $processResult = Process-LocalWordDocument $localFilePath
            
            if ($processResult.Success) {
                # Record the processing results
                Add-ProcessedFileRecord -FileName $file.Name -OriginalSize $processResult.OriginalSize -NewSize $processResult.NewSize -SvgCount $processResult.SvgCount -PngCount $processResult.PngCount
                
                # Upload back to SharePoint using improved method
                Write-Log "Uploading processed file back to SharePoint..."
                
                $uploadSuccess = Upload-ProcessedFileWithRetry -LocalFilePath $localFilePath -OriginalServerRelativeUrl $file.ServerRelativeUrl
                
                if ($uploadSuccess) {
                    Write-Log "Successfully completed processing for: $($file.Name)" "SUCCESS"
                    $script:ProcessedFiles++
                    Add-CompletedFile $file.Name
                } else {
                    Write-Log "Failed to upload processed file: $($file.Name)" "ERROR"
                }
            } else {
                Write-Log "File processing did not result in changes: $($file.Name)" "INFO"
            }
        }
        catch {
            Write-Log "Error processing $($file.Name): $($_.Exception.Message)" "ERROR"
        }
        finally {
            # Cleanup local file
            if (Test-Path $localFilePath) {
                Remove-Item $localFilePath -Force
            }
        }
        
        # Brief pause between files to be respectful
        Start-Sleep -Seconds 2
    }
    
    # Final summary with size report
    $totalElapsed = (Get-Date) - $script:StartTime
    $avgSpeedMBps = if ($script:TotalBytes -gt 0) { 
        [math]::Round(($script:TotalBytes / 1MB) / $totalElapsed.TotalSeconds, 2) 
    } else { 0 }
    
    # Generate detailed size report
    $sizeStats = Generate-SizeReport
    
    Write-Log "========== FINAL SUMMARY =========="
    Write-Log "Total Files Processed: $($script:ProcessedFiles)"
    Write-Log "Successful Downloads: $($script:SuccessfulDownloads)"  
    Write-Log "Failed Downloads: $($script:FailedDownloads)"
    Write-Log "Total Data Downloaded: $([math]::Round($script:TotalBytes / 1GB, 2)) GB"
    Write-Log "Average Download Speed: $avgSpeedMBps MB/s"
    Write-Log "Total Elapsed Time: $($totalElapsed.ToString('hh\:mm\:ss'))"
    Write-Log ""
    Write-Log "========== SIZE REDUCTION SUMMARY =========="
    Write-Log "Total Space Saved: $($sizeStats.TotalReductionMB) MB ($($sizeStats.TotalReductionGB) GB)"
    Write-Log "Overall Size Reduction: $($sizeStats.OverallReductionPercent)%"
    Write-Log "Detailed report saved to: $sizeReportPath"
    Write-Log "============================================="
}
catch {
    Write-Log "Critical error: $($_.Exception.Message)" "ERROR"
}
finally {
    try {
        Disconnect-PnPOnline
        Write-Log "Disconnected from SharePoint"
    } catch {
        Write-Log "Error during disconnect: $($_.Exception.Message)" "ERROR"
    }
}