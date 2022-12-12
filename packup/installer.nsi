;NSIS Modern User Interface
;Basic Example Script
;Written by Joost Verburg

;--------------------------------
;Include Modern UI

  !include "MUI2.nsh"

  !define MUI_ICON "csv_viewer.ico"
  !define MUI_UNICON "csv_viewer.ico"

;--------------------------------
;General

  ;Name and file
  Name "CsvViewer"
  OutFile "CsvViewerInstaller.exe"
  Unicode True

  ;Default installation folder
  InstallDir "$LOCALAPPDATA\CsvViewer"
  
  ;Get installation folder from registry if available
  InstallDirRegKey HKCU "Software\CsvViewer" ""

  ;Request application privileges for Windows Vista
  RequestExecutionLevel user

;--------------------------------
;Interface Settings

  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_LICENSE "LICENSE"
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_INSTFILES
  !insertmacro MUI_PAGE_FINISH
  
  !insertmacro MUI_UNPAGE_WELCOME
  !insertmacro MUI_UNPAGE_CONFIRM
  !insertmacro MUI_UNPAGE_INSTFILES
  !insertmacro MUI_UNPAGE_FINISH  

;--------------------------------
;Languages
 
  !insertmacro MUI_LANGUAGE "SimpChinese"

;--------------------------------
;Version Information

  VIProductVersion "1.1.0.0"
  VIFileVersion "1.1.0.0"
  
  VIAddVersionKey "ProductName" "CsvViewer"
  VIAddVersionKey "Comments" "A simple csv viewer."
  VIAddVersionKey "CompanyName" "Litmonkey"
  VIAddVersionKey "LegalCopyright" "Copyright Roser"
  VIAddVersionKey "FileDescription" "A simple csv viewer."
  VIAddVersionKey "FileVersion" "1.1.0"

;--------------------------------
;Installer Sections

Section "Dummy Section" SecDummy

  SetOutPath "$INSTDIR"
  
  File /r "CsvViewer\*"
  
  ;Store installation folder
  WriteRegStr HKCU "Software\CsvViewer" "" $INSTDIR
  
  ;Create uninstaller
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  CreateShortcut "$DESKTOP\CsvViewer.lnk" "$INSTDIR\CsvViewer.exe"

SectionEnd

;--------------------------------
;Uninstaller Section

Section "Uninstall"

  Delete "$INSTDIR\Uninstall.exe"

  RMDir /r "$INSTDIR"

  DeleteRegKey /ifempty HKCU "Software\CsvViewer" ""

SectionEnd