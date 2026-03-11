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
    (str(project_root / "config.example.json"), "."),
    (str(project_root / "version.txt"), "."),
]

excluded_modules = [
    # Optional ML stacks pulled by transitive packages but not required
    # for the desktop client/runtime release.
    "torch",
    "torchvision",
    "torchaudio",
    "tensorflow",
    "transformers",
    "scipy",
    "sklearn",
    "sentence_transformers",
    "jax",
    "jaxlib",
    "matplotlib",
    "pandas",
    "sympy",
    "triton",
    "expecttest",
    # The app uses PySide6 only. Exclude other Qt bindings if they happen
    # to be installed in the build environment.
    "PyQt5",
    "PyQt6",
    "PySide2",
]


a = Analysis(
    [str(project_root / "main.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excluded_modules,
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
    icon=str(project_root / "ui" / "assets" / "app" / "escgram.ico"),
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
