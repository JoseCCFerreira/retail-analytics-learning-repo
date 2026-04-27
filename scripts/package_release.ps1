$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$projectName = Split-Path -Leaf $root
$distDir = Join-Path $root 'dist'
$stageDir = Join-Path $distDir ($projectName + '-package')
$zipPath = Join-Path $distDir ($projectName + '-package.zip')

if (Test-Path $stageDir) {
    Remove-Item $stageDir -Recurse -Force
}
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

New-Item -ItemType Directory -Path $stageDir | Out-Null

$excludePatterns = @(
    '\.venv($|\\)',
    '\.git($|\\)',
    '\\dist($|\\)',
    '\\__pycache__($|\\)',
    '\\.pytest_cache($|\\)',
    '\\.tmp_dbt_profiles($|\\)'
)

Get-ChildItem -Path $root -Recurse -Force | Where-Object {
    $fullName = $_.FullName
    foreach ($pattern in $excludePatterns) {
        if ($fullName -match $pattern) {
            return $false
        }
    }
    return $true
} | ForEach-Object {
    $relative = $_.FullName.Substring($root.Length).TrimStart('\')
    $destination = Join-Path $stageDir $relative

    if ($_.PSIsContainer) {
        if (-not (Test-Path $destination)) {
            New-Item -ItemType Directory -Path $destination | Out-Null
        }
    }
    else {
        $parent = Split-Path -Parent $destination
        if (-not (Test-Path $parent)) {
            New-Item -ItemType Directory -Path $parent | Out-Null
        }
        Copy-Item $_.FullName -Destination $destination -Force
    }
}

Compress-Archive -Path (Join-Path $stageDir '*') -DestinationPath $zipPath -Force
Write-Host "Package created: $zipPath"
