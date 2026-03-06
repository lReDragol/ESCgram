#define AppName "ESCgram"
#define AppVersion "0.2.5"
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
SetupIconFile=..\..\ui\assets\app\escgram.ico

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\\Russian.isl"

[Tasks]
Name: "desktopicon"; Description: "Создать ярлык на рабочем столе"; GroupDescription: "Ярлыки:"; Flags: unchecked

[Files]
Source: "..\\..\\dist\\ESCgram\\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\\{#AppName}"; Filename: "{app}\\{#AppExeName}"; Parameters: "--data-dir ""{code:GetDataDir}"""; IconFilename: "{app}\\ui\\assets\\app\\escgram.ico"
Name: "{autodesktop}\\{#AppName}"; Filename: "{app}\\{#AppExeName}"; Parameters: "--data-dir ""{code:GetDataDir}"""; Tasks: desktopicon; IconFilename: "{app}\\ui\\assets\\app\\escgram.ico"

[Code]
var
  DataDirPage: TInputDirWizardPage;
  ApiPage: TInputQueryWizardPage;

function _JsonEscape(const S: String): String; forward;

function _FindCharFrom(const S: String; const Ch: Char; const StartPos: Integer): Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := StartPos to Length(S) do
  begin
    if S[I] = Ch then
    begin
      Result := I;
      Exit;
    end;
  end;
end;

function _ReadBootstrapDataDir: String;
var
  BootstrapPath: String;
  Content: String;
  KeyPos: Integer;
  ColonPos: Integer;
  QuoteStart: Integer;
  QuoteEnd: Integer;
begin
  Result := '';
  BootstrapPath := ExpandConstant('{userappdata}\\DragoGUI\\bootstrap.json');
  if not FileExists(BootstrapPath) then
    Exit;
  Content := '';
  LoadStringFromFile(BootstrapPath, Content);
  if Trim(Content) = '' then
    Exit;

  KeyPos := Pos('"data_dir"', Content);
  if KeyPos <= 0 then
    Exit;
  ColonPos := _FindCharFrom(Content, ':', KeyPos + 10);
  if ColonPos <= 0 then
    Exit;
  QuoteStart := _FindCharFrom(Content, '"', ColonPos + 1);
  if QuoteStart <= 0 then
    Exit;
  QuoteEnd := _FindCharFrom(Content, '"', QuoteStart + 1);
  if QuoteEnd <= QuoteStart then
    Exit;

  Result := Copy(Content, QuoteStart + 1, QuoteEnd - QuoteStart - 1);
end;

procedure _WriteBootstrapDataDir(const DataDir: String);
var
  BootstrapDir: String;
  BootstrapPath: String;
  SafeDir: String;
  Payload: String;
begin
  SafeDir := Trim(DataDir);
  if SafeDir = '' then
    Exit;
  BootstrapDir := ExpandConstant('{userappdata}\\DragoGUI');
  if not DirExists(BootstrapDir) then
    ForceDirectories(BootstrapDir);
  BootstrapPath := AddBackslash(BootstrapDir) + 'bootstrap.json';
  SafeDir := _JsonEscape(SafeDir);
  Payload := '{'#13#10 + '  "data_dir": "' + SafeDir + '"'#13#10 + '}';
  SaveStringToFile(BootstrapPath, Payload, False);
end;

procedure InitializeWizard;
var
  DefaultDataDir: String;
  ParamDataDir: String;
  BootstrapDataDir: String;
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
  DefaultDataDir := ExpandConstant('{localappdata}\\ESCgram');
  ParamDataDir := Trim(ExpandConstant('{param:DATADIR|}'));
  if ParamDataDir <> '' then
    DefaultDataDir := ParamDataDir
  else
  begin
    BootstrapDataDir := _ReadBootstrapDataDir;
    if BootstrapDataDir <> '' then
      DefaultDataDir := BootstrapDataDir;
  end;
  DataDirPage.Values[0] := DefaultDataDir;

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

function _ConfigPathForDataDir(const DataDir: String): String;
var
  DirValue: String;
begin
  DirValue := Trim(DataDir);
  if DirValue = '' then
  begin
    Result := '';
    Exit;
  end;
  Result := AddBackslash(DirValue) + 'config.json';
end;

function _ConfigExistsForSelectedDataDir: Boolean;
var
  ConfigPath: String;
begin
  ConfigPath := _ConfigPathForDataDir(DataDirPage.Values[0]);
  Result := (ConfigPath <> '') and FileExists(ConfigPath);
end;

function _LooksLikeUpgradeInstall: Boolean;
var
  ExistingExe: String;
begin
  ExistingExe := AddBackslash(WizardDirValue()) + '{#AppExeName}';
  Result := FileExists(ExistingExe);
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
    if _LooksLikeUpgradeInstall or _ConfigExistsForSelectedDataDir then
      Exit;

    ApiIdValue := Trim(ApiPage.Values[0]);
    ApiHashValue := Trim(ApiPage.Values[1]);
    if (ApiIdValue = '') and (ApiHashValue = '') then
    begin
      MsgBox('Введите API ID и API Hash Telegram.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
    if (ApiIdValue = '') or (ApiHashValue = '') then
    begin
      MsgBox('Нужно заполнить оба поля: API ID и API Hash.', mbError, MB_OK);
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

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  Result := False;
  if PageID = ApiPage.ID then
    Result := _LooksLikeUpgradeInstall or _ConfigExistsForSelectedDataDir;
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
  begin
    _WriteConfigIfMissing;
    _WriteBootstrapDataDir(DataDirPage.Values[0]);
  end;
end;
