param($installPath, $toolsPath, $package, $project)

$dir = split-Path $MyInvocation.MyCommand.Path;
& "$dir\d48e77e2a7fa40089165a83711248dc2.ps1" $installPath $toolsPath $package $project;

