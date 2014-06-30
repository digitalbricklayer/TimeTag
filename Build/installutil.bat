@echo off
Rem .NET on Windows 64 has a different installation folder than on Windows 32
IF NOT EXIST %systemroot%\Microsoft.NET\Framework64\ (GOTO windows32) ELSE (GOTO windows64)

:windows32
%systemroot%\Microsoft.NET\Framework\v2.0.50727\installutil %1 %2 %3 %4 %5 %6
GOTO end

:windows64
%systemroot%\Microsoft.NET\Framework64\v2.0.50727\installutil %1 %2 %3 %4 %5 %6

:end
