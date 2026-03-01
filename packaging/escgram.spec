# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path


spec_dir = Path(globals().get("SPECPATH", Path.cwd())).resolve()
if spec_dir.is_file():
    spec_dir = spec_dir.parent
project_root = spec_dir.parent if spec_dir.name.lower() == "packaging" else spec_dir

app_name = "ESCgram"

datas = [
    (str(project_root / "ui" / "assets"), "ui/assets"),
    (str(project_root / "ui" / "styles.json"), "ui"),
]


a = Analysis(
    ["main.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name=app_name,
)
