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
  ApiPage: TInputQueryWizardPage;

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

  ApiPage := CreateInputQueryPage(
    DataDirPage.ID,
    'Telegram API',
    'Введите API ID и API Hash',
    'Без этих данных Telegram-вход не работает. Получить их можно на my.telegram.org.'
  );
  ApiPage.Add('API ID:', False);
  ApiPage.Add('API Hash:', True);
end;

function GetDataDir(Param: String): String;
begin
  Result := DataDirPage.Values[0];
end;

function _IsDigitsOnly(const S: String): Boolean;
var
  I: Integer;
begin
  Result := (Length(S) > 0);
  if not Result then
    Exit;
  for I := 1 to Length(S) do
  begin
    if (S[I] < '0') or (S[I] > '9') then
    begin
      Result := False;
      Exit;
    end;
  end;
end;

function _JsonEscape(const S: String): String;
begin
  Result := S;
  StringChangeEx(Result, '\', '\\', True);
  StringChangeEx(Result, '"', '\"', True);
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  ApiIdValue: String;
  ApiHashValue: String;
begin
  Result := True;
  if CurPageID = ApiPage.ID then
  begin
    ApiIdValue := Trim(ApiPage.Values[0]);
    ApiHashValue := Trim(ApiPage.Values[1]);
    if (ApiIdValue = '') or (ApiHashValue = '') then
    begin
      MsgBox('Введите API ID и API Hash Telegram.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
    if not _IsDigitsOnly(ApiIdValue) then
    begin
      MsgBox('API ID должен содержать только цифры.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
  end;
end;

procedure _WriteConfigIfMissing;
var
  DataDir: String;
  ConfigPath: String;
  ApiIdValue: String;
  ApiHashValue: String;
  ConfigContent: String;
begin
  DataDir := Trim(DataDirPage.Values[0]);
  if DataDir = '' then
    Exit;

  if not DirExists(DataDir) then
    ForceDirectories(DataDir);

  ConfigPath := AddBackslash(DataDir) + 'config.json';
  if FileExists(ConfigPath) then
    Exit;

  ApiIdValue := Trim(ApiPage.Values[0]);
  ApiHashValue := Trim(ApiPage.Values[1]);
  if (ApiIdValue = '') or (ApiHashValue = '') then
    Exit;

  ConfigContent :=
    '{'#13#10 +
    '  "telegram_api_id": ' + ApiIdValue + ','#13#10 +
    '  "telegram_api_hash": "' + _JsonEscape(ApiHashValue) + '",'#13#10 +
    '  "allowed_users": [],'#13#10 +
    '  "admin_users": [],'#13#10 +
    '  "chat_ids": [],'#13#10 +
    '  "strict_live_filter": false,'#13#10 +
    '  "socket_host": "127.0.0.1",'#13#10 +
    '  "socket_port": 8765,'#13#10 +
    '  "transcription_model": "base"'#13#10 +
    '}';
  SaveStringToFile(ConfigPath, ConfigContent, False);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
    _WriteConfigIfMissing;
end;
