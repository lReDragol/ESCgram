#define AppName "ESCgram"
#define AppVersion "0.2.11"
#define AppPublisher "Drago"
#define AppExeName "ESCgram.exe"
#define AppUninstallKey "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{A1B5F40B-6E07-4C2F-9C2C-1B89B85E3F6B}_is1"

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
const
  InstallModeUpdate = 0;
  InstallModeUninstall = 1;
  InstallModeAnotherDir = 2;

var
  ExistingInstallPage: TWizardPage;
  ExistingInstallLabel: TNewStaticText;
  ExistingModeUpdate: TNewRadioButton;
  ExistingModeUninstall: TNewRadioButton;
  ExistingModeAnotherDir: TNewRadioButton;
  ExistingInstallFound: Boolean;
  ExistingInstallDir: String;
  ExistingUninstallCmd: String;
  DataDirPage: TInputDirWizardPage;
  ApiPage: TInputQueryWizardPage;

function _JsonEscape(const S: String): String; forward;
function _GetSelectedInstallMode: Integer; forward;
function _RunExistingUninstaller: Boolean; forward;

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
begin
  // Keep installer parser simple and robust across Inno compiler variants.
  // Data dir for upgrades is passed explicitly via /DATADIR by auto-update flow.
  Result := '';
end;

function _TryReadInstalledString(const ValueName: String; var Value: String): Boolean;
begin
  Result :=
    RegQueryStringValue(HKCU, '{#AppUninstallKey}', ValueName, Value) or
    RegQueryStringValue(HKLM, '{#AppUninstallKey}', ValueName, Value);
end;

function _TrimTrailingSlash(const Value: String): String;
begin
  Result := Trim(Value);
  while (Length(Result) > 3) and ((Result[Length(Result)] = '\') or (Result[Length(Result)] = '/')) do
    Delete(Result, Length(Result), 1);
end;

function _SplitCommandLine(const CommandLine: String; var FileName: String; var Params: String): Boolean;
var
  Raw: String;
  PosSpace: Integer;
  PosQuote: Integer;
begin
  Result := False;
  FileName := '';
  Params := '';
  Raw := Trim(CommandLine);
  if Raw = '' then
    Exit;

  if Raw[1] = '"' then
  begin
    PosQuote := _FindCharFrom(Raw, '"', 2);
    if PosQuote <= 1 then
      Exit;
    FileName := Copy(Raw, 2, PosQuote - 2);
    Params := Trim(Copy(Raw, PosQuote + 1, MaxInt));
  end
  else
  begin
    PosSpace := Pos(' ', Raw);
    if PosSpace > 0 then
    begin
      FileName := Copy(Raw, 1, PosSpace - 1);
      Params := Trim(Copy(Raw, PosSpace + 1, MaxInt));
    end
    else
      FileName := Raw;
  end;

  Result := Trim(FileName) <> '';
end;

procedure _DetectExistingInstall;
begin
  ExistingInstallFound := False;
  ExistingInstallDir := '';
  ExistingUninstallCmd := '';

  if _TryReadInstalledString('UninstallString', ExistingUninstallCmd) then
    ExistingInstallFound := Trim(ExistingUninstallCmd) <> '';

  if _TryReadInstalledString('Inno Setup: App Path', ExistingInstallDir) then
    ExistingInstallFound := True
  else if _TryReadInstalledString('InstallLocation', ExistingInstallDir) then
    ExistingInstallFound := True;

  ExistingInstallDir := _TrimTrailingSlash(ExistingInstallDir);
  if (ExistingInstallDir = '') and ExistingInstallFound then
    ExistingInstallDir := ExpandConstant('{autopf}\\{#AppName}');
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
  _DetectExistingInstall;

  ExistingInstallPage := CreateCustomPage(
    wpWelcome,
    'Обнаружена установленная версия ESCgram',
    'Выберите действие для найденной установки.'
  );
  ExistingInstallLabel := TNewStaticText.Create(ExistingInstallPage);
  ExistingInstallLabel.Parent := ExistingInstallPage.Surface;
  ExistingInstallLabel.Left := 0;
  ExistingInstallLabel.Top := 0;
  ExistingInstallLabel.Width := ExistingInstallPage.SurfaceWidth;
  ExistingInstallLabel.AutoSize := False;
  ExistingInstallLabel.WordWrap := True;
  if ExistingInstallDir <> '' then
    ExistingInstallLabel.Caption :=
      'Найдена установленная копия ESCgram:'#13#10 + ExistingInstallDir + #13#10#13#10 +
      'Можно обновить её, удалить или установить новую копию в другую папку.'
  else
    ExistingInstallLabel.Caption :=
      'Найдена установленная копия ESCgram.'#13#10#13#10 +
      'Можно обновить её, удалить или установить новую копию в другую папку.';
  ExistingInstallLabel.Height := ScaleY(68);

  ExistingModeUpdate := TNewRadioButton.Create(ExistingInstallPage);
  ExistingModeUpdate.Parent := ExistingInstallPage.Surface;
  ExistingModeUpdate.Left := 0;
  ExistingModeUpdate.Top := ExistingInstallLabel.Top + ExistingInstallLabel.Height + ScaleY(8);
  ExistingModeUpdate.Width := ExistingInstallPage.SurfaceWidth;
  ExistingModeUpdate.Caption := 'Обновить текущую установку';
  ExistingModeUpdate.Checked := True;

  ExistingModeUninstall := TNewRadioButton.Create(ExistingInstallPage);
  ExistingModeUninstall.Parent := ExistingInstallPage.Surface;
  ExistingModeUninstall.Left := 0;
  ExistingModeUninstall.Top := ExistingModeUpdate.Top + ExistingModeUpdate.Height + ScaleY(8);
  ExistingModeUninstall.Width := ExistingInstallPage.SurfaceWidth;
  ExistingModeUninstall.Caption := 'Удалить установленную версию';

  ExistingModeAnotherDir := TNewRadioButton.Create(ExistingInstallPage);
  ExistingModeAnotherDir.Parent := ExistingInstallPage.Surface;
  ExistingModeAnotherDir.Left := 0;
  ExistingModeAnotherDir.Top := ExistingModeUninstall.Top + ExistingModeUninstall.Height + ScaleY(8);
  ExistingModeAnotherDir.Width := ExistingInstallPage.SurfaceWidth;
  ExistingModeAnotherDir.Caption := 'Установить в другую папку';

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

function _GetSelectedInstallMode: Integer;
begin
  Result := InstallModeUpdate;
  if ExistingModeUninstall.Checked then
    Result := InstallModeUninstall
  else if ExistingModeAnotherDir.Checked then
    Result := InstallModeAnotherDir;
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

function _RunExistingUninstaller: Boolean;
var
  UninstallerExe: String;
  UninstallerParams: String;
  ExtraParams: String;
  ResultCode: Integer;
begin
  Result := False;
  if Trim(ExistingUninstallCmd) = '' then
  begin
    MsgBox('Не удалось найти команду удаления установленной версии.', mbError, MB_OK);
    Exit;
  end;
  if not _SplitCommandLine(ExistingUninstallCmd, UninstallerExe, UninstallerParams) then
  begin
    MsgBox('Не удалось разобрать команду удаления установленной версии.', mbError, MB_OK);
    Exit;
  end;

  ExtraParams := Trim(UninstallerParams + ' /VERYSILENT /SUPPRESSMSGBOXES /NORESTART');
  Result := Exec(UninstallerExe, ExtraParams, '', SW_SHOWNORMAL, ewWaitUntilTerminated, ResultCode);
  if not Result then
  begin
    MsgBox('Не удалось запустить удаление установленной версии.', mbError, MB_OK);
    Exit;
  end;
  if ResultCode <> 0 then
  begin
    MsgBox('Удаление завершилось с кодом ' + IntToStr(ResultCode) + '.', mbError, MB_OK);
    Result := False;
    Exit;
  end;

  MsgBox('Установленная версия ESCgram удалена.', mbInformation, MB_OK);
  Result := True;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  ApiIdValue: String;
  ApiHashValue: String;
begin
  Result := True;
  if ExistingInstallFound and (CurPageID = ExistingInstallPage.ID) then
  begin
    case _GetSelectedInstallMode of
      InstallModeUpdate:
        begin
          if ExistingInstallDir <> '' then
            WizardForm.DirEdit.Text := ExistingInstallDir;
        end;
      InstallModeUninstall:
        begin
          Result := False;
          if MsgBox('Удалить найденную установку ESCgram сейчас?', mbConfirmation, MB_YESNO) = IDYES then
          begin
            if _RunExistingUninstaller then
              WizardForm.Close;
          end;
          Exit;
        end;
    end;
  end;

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
  if PageID = ExistingInstallPage.ID then
    Result := not ExistingInstallFound
  else if PageID = wpSelectDir then
    Result := ExistingInstallFound and (_GetSelectedInstallMode = InstallModeUpdate)
  else if PageID = ApiPage.ID then
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
