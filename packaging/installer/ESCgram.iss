#define AppName "ESCgram"
#define AppVersion "0.1.0"
#define AppPublisher "Drago"
#define AppExeName "ESCgram.exe"

[Setup]
AppId={{A1B5F40B-6E07-4C2F-9C2C-1B89B85E3F6B}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=..\..\dist_installer
OutputBaseFilename=ESCgram-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\\Russian.isl"

[Tasks]
Name: "desktopicon"; Description: "Создать ярлык на рабочем столе"; GroupDescription: "Ярлыки:"; Flags: unchecked

[Files]
Source: "..\\..\\dist\\ESCgram\\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\\{#AppName}"; Filename: "{app}\\{#AppExeName}"; Parameters: "--data-dir ""{code:GetDataDir}"""
Name: "{autodesktop}\\{#AppName}"; Filename: "{app}\\{#AppExeName}"; Parameters: "--data-dir ""{code:GetDataDir}"""; Tasks: desktopicon

[Code]
var
  DataDirPage: TInputDirWizardPage;

procedure InitializeWizard;
begin
  DataDirPage := CreateInputDirPage(
    wpSelectDir,
    'Папка данных',
    'Где хранить историю и файлы',
    'Выберите папку, где ESCgram будет хранить историю, настройки, медиа и сессии Telegram.',
    False,
    ''
  );
  DataDirPage.Add('');
  DataDirPage.Values[0] := ExpandConstant('{localappdata}\\ESCgram');
end;

function GetDataDir(Param: String): String;
begin
  Result := DataDirPage.Values[0];
end;
